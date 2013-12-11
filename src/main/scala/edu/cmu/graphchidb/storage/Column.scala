package edu.cmu.graphchidb.storage

import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
import edu.cmu.graphchidb.storage.ByteConverters._

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.io._
import java.sql.DriverManager
import edu.cmu.graphchi.preprocessing.VertexIdTranslate
import java.nio.{MappedByteBuffer, LongBuffer, ByteBuffer}
import scala.Some
import java.util.concurrent.atomic.AtomicLong
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 *
 *
 * Database column
 * @author Aapo Kyrola
 */

trait Column[T] {

  def columnId: Int

  def encode(value: T, out: ByteBuffer) : Unit
  def decode(in: ByteBuffer) : T
  def elementSize: Int

  def get(idx: Long) : Option[T]
  def getMany(idxs: Set[java.lang.Long]) : Map[java.lang.Long, Option[T]] = {
    idxs.map(idx => idx -> get(idx)).toMap
  }
  def getName(idx: Long) : Option[String]
  def set(idx: Long, value: T) : Unit
  def update(idx: Long, updateFunc: Option[T] => T) : Unit = {
    val cur = get(idx)
    set(idx, updateFunc(cur))
  }
  def indexing: DatabaseIndexing

  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit

  def recreateWithData(shardNum: Int, data: Array[Byte]) : Unit

}

class FileColumn[T](id: Int, filePrefix: String, sparse: Boolean, _indexing: DatabaseIndexing,
                    converter: ByteConverter[T]) extends Column[T] {

  override def columnId = id

  def encode(value: T, out: ByteBuffer) = converter.toBytes(value, out)
  def decode(in: ByteBuffer) = converter.fromBytes(in)
  def elementSize = converter.sizeOf

  def indexing = _indexing
  def blockFilename(shardNum: Int) = filePrefix + "." + shardNum


  var blocks = (0 until indexing.nShards).map {
    shard =>
      if (!sparse) {
        new  MemoryMappedDenseByteStorageBlock(new File(blockFilename(shard)), None,
          converter.sizeOf) with DataBlock[T]
      } else {
        throw new NotImplementedException()
      }
  }.toArray


  private def checkSize(block: MemoryMappedDenseByteStorageBlock, localIdx: Int) = {
    if (block.size <= localIdx && _indexing.allowAutoExpansion) {
      val EXPANSION_BLOCK = 4096 // TODO -- not right place
      val expansionSize = (scala.math.ceil((localIdx + 1).toDouble /  EXPANSION_BLOCK) * EXPANSION_BLOCK).toInt
      block.expand(expansionSize)

      //println("Expanding because %d was out of bounds. New size: %d / %d".format(localIdx, expansionSize, block.size))
    }
  }

  /* Internal call */
  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit = blocks(shardNum).readIntoBuffer(idx, buf)
  def get(idx: Long) =  {
    val block = blocks(indexing.shardForIndex(idx))
    val localIdx = indexing.globalToLocal(idx).toInt
    checkSize(block, localIdx)
    block.get(localIdx)(converter)
  }
  def getName(idx: Long) = get(idx).map(a => a.toString).headOption

  def set(idx: Long, value: T) : Unit = {
    val block = blocks(indexing.shardForIndex(idx))
    val localIdx = indexing.globalToLocal(idx).toInt
    checkSize(block, localIdx)
    block.set(localIdx, value)(converter)
  }

  def update(idx: Long, updateFunc: Option[T] => T, byteBuffer: ByteBuffer) : Unit = {
    val block =  blocks(indexing.shardForIndex(idx))
    val localIdx = indexing.globalToLocal(idx).toInt
    checkSize(block, localIdx)
    byteBuffer.rewind()
    val curVal = block.get(localIdx, byteBuffer)(converter)
    byteBuffer.rewind()
    block.set(localIdx, updateFunc(curVal), byteBuffer)(converter)
  }

  override def update(idx: Long, updateFunc: Option[T] => T) : Unit = {
    val buffer = ByteBuffer.allocate(converter.sizeOf)
    update(idx, updateFunc, buffer)
  }

  /* Create data from scratch */
  def recreateWithData(shardNum: Int, data: Array[Byte]) : Unit = {
    blocks(shardNum) = blocks(shardNum).createNew[T](data)
  }
}

class CategoricalColumn(id: Int, filePrefix: String, indexing: DatabaseIndexing, values: IndexedSeq[String])
  extends FileColumn[Byte](id, filePrefix, sparse=false, indexing, ByteByteConverter) {

  def byteToInt(b: Byte) : Int = if (b < 0) 256 + b  else b

  def categoryName(b: Byte) : String = values(byteToInt(b))
  def categoryName(i: Int)  : String = values(i)
  def indexForName(name: String) = values.indexOf(name).toByte


  def set(idx: Long, valueName: String) : Unit = super.set(idx, indexForName(valueName).toByte)
  override def getName(idx: Long) : Option[String] = get(idx).map( vidx => categoryName(byteToInt(vidx))).headOption

}


/** Vardata columns have two parts: one is long-column holding indices to the
  * var data payload, which is stored in a special log (TODO: garbage collection).
  */
class VarDataColumn(name: String,  filePrefix: String, _pointerColumn: Column[Long], blobType: String)   {
  val logFileName = filePrefix +  ".vardata_" + name + "_" + _pointerColumn.indexing.name
  val logIdxFile = logFileName + ".idx"

  println(logFileName)
  println(logIdxFile)

  val pointerColumn = _pointerColumn

  private val bufferSize = 100000
  private var bufferIdStart = 0L
  private var bufferIndexStart = 0L
  private var bufferCounter = 0

  private val buffer = new ByteArrayOutputStream(bufferSize * 100) {
    def currentBuf = buf
  }

  var initialized = false

  val bufferIndices = new Array[Long](bufferSize)
  val logOutput = new RandomAccessFile(logFileName, "rw")
  val indexFile = new RandomAccessFile(logIdxFile, "rw")
  val idSeq = new AtomicLong()

  var indexBuffer : MappedByteBuffer = null
  var dataBuffer : MappedByteBuffer = null

  val lock = new ReentrantReadWriteLock()

  def init() {
    this.synchronized {
      if (new File(logIdxFile).exists()) {
        idSeq.set(new File(logIdxFile).length / 8)
        bufferIdStart = idSeq.get
        bufferIndexStart = new File(logFileName).length()
        indexBuffer = indexFile.getChannel.map(MapMode.READ_ONLY, 0, indexFile.length())
        dataBuffer = logOutput.getChannel.map(MapMode.READ_ONLY, 0, logOutput.length())

      }
      println("Var data index start: " + bufferIdStart + " / " + bufferIndexStart)
      initialized = true

    }
  }

  init()

  def flushBuffer(): Unit = {
    logOutput.seek(logOutput.length())
    logOutput.write(buffer.toByteArray)

    val byteBuffer = ByteBuffer.allocate(bufferIndices.length * 8)
    val longBuffer = byteBuffer.asLongBuffer()
    longBuffer.put(bufferIndices)
    val flushedIndicesAsBytes = byteBuffer.array()
    indexFile.seek(indexFile.length())
    indexFile.write(flushedIndicesAsBytes, 0, bufferCounter * 8)

    bufferCounter = 0
    bufferIndexStart = new File(logFileName).length()
    bufferIdStart = idSeq.get
    buffer.reset()
    indexBuffer = indexFile.getChannel.map(MapMode.READ_ONLY, 0, indexFile.length())
    dataBuffer = logOutput.getChannel.map(MapMode.READ_ONLY, 0, logOutput.length())

    println("flush: " + new File(logFileName).getAbsolutePath + ", " +  new File(logFileName).length())
  }

  def insert(data: Array[Byte]) : Long = {
    lock.writeLock().lock
    try {
      if (!initialized) throw new IllegalStateException("Not initialized")
      bufferIndices(bufferCounter) = bufferIndexStart + buffer.size()
      bufferCounter += 1
      buffer.write(data)
      val id = idSeq.getAndIncrement

      if (bufferCounter == bufferSize) {
        flushBuffer
      }
      id
    } finally {
       lock.writeLock().unlock()
    }
  }

  def get(id: Long) : Array[Byte] = {
    lock.readLock().lock()
    try {
      if (id >= bufferIdStart) {
        // Look from buffers
        val idOff = (id - bufferIdStart).toInt
        val bufferOff = bufferIndices(idOff)
        val len = if (idOff < bufferCounter - 1) { bufferIndices(idOff + 1) - bufferOff} else
        { buffer.size() - (bufferOff - bufferIndexStart) }

        val res = new Array[Byte](len.toInt)
        try {
          Array.copy(buffer.currentBuf, (bufferOff - bufferIndexStart).toInt, res, 0, len.toInt)
        } catch {
          case aie: Exception  =>
            throw aie
        }
        res
      } else {
        // Seek file
        val fileOff = indexBuffer.getLong((id * 8).toInt)
        val next = if (id * 8 < indexBuffer.capacity() - 8) { indexBuffer.getLong((id * 8 + 8).toInt) } else
            { logOutput.length() }
        val len = next - fileOff

        val res = new Array[Byte](len.toInt)

        val tmpBuffer = dataBuffer.duplicate()
        tmpBuffer.position(fileOff.toInt)
        tmpBuffer.get(res)
        res
      }
    } finally {
      lock.readLock().unlock()
    }
  }

  def delete(id: Long) : Unit = {
    // Not implemented now
  }
}


class MySQLBackedColumn[T](id: Int, tableName: String, columnName: String, _indexing: DatabaseIndexing,
                           vertexIdTranslate: VertexIdTranslate) extends Column[T] {

  def encode(value: T, out: ByteBuffer) = throw new UnsupportedOperationException
  def decode(in: ByteBuffer) = throw new UnsupportedOperationException
  def elementSize = throw new UnsupportedOperationException

  override def columnId = id

  override  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit = throw new UnsupportedOperationException

  // TODO: temporary code
  val dbConnection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://127.0.0.1/graphchidb?" +
      "user=graphchidb&password=dbchi9999")
  }

  def get(_idx: Long) = {
    val idx = vertexIdTranslate.backward(_idx)
    val pstmt = dbConnection.prepareStatement("select %s from %s where id=?".format(columnName, tableName))
    try {
      pstmt.setLong(1, idx)
      val rs = pstmt.executeQuery()
      if (rs.next()) Some(rs.getObject(1).asInstanceOf[T]) else None
    } finally {
      if (pstmt != null) pstmt.close()
    }
  }

  override def getName(idx: Long) = get(idx).map(a => a.toString).headOption


  override def getMany(_idxs: Set[java.lang.Long]) : Map[java.lang.Long, Option[T]] = {
    val idxs = new Array[Long](_idxs.size)
    _idxs.zipWithIndex.foreach { tp=> { idxs(tp._2) = vertexIdTranslate.backward(tp._1) } }

    val pstmt = dbConnection.prepareStatement("select id, %s from %s where id in (%s)".format(columnName, tableName,
      idxs.mkString(",")))
    try {
      val rs = pstmt.executeQuery()

      new Iterator[(java.lang.Long, Option[T])] {
        def hasNext = rs.next()
        def next() = (vertexIdTranslate.forward(rs.getLong(1)), Some(rs.getObject(2).asInstanceOf[T]))
      }.toStream.toMap

    } finally {
      if (pstmt != null) pstmt.close()
    }
  }

  def set(_idx: Long, value: T) = {
    val idx = vertexIdTranslate.backward(_idx)
    val pstmt = dbConnection.prepareStatement("replace into %s (id, %s) values(?, ?)".format(tableName, columnName))
    try {
      pstmt.setLong(1, idx)
      pstmt.setObject(2, value)
      pstmt.executeUpdate()
    } finally {
      if (pstmt != null) pstmt.close()
    }
  }

  def getByName(name: String) : Option[Long] = {
    val pstmt = dbConnection.prepareStatement("select id from %s where %s=?".format(tableName, columnName))
    try {
      pstmt.setString(1, name)
      val rs = pstmt.executeQuery()
      if (rs.next) Some(vertexIdTranslate.forward(rs.getLong(1))) else None
    } finally {
      if (pstmt != null) pstmt.close()
    }
  }

  def indexing = _indexing

  def recreateWithData(shardNum: Int, data: Array[Byte]) : Unit= throw new NotImplementedException
}