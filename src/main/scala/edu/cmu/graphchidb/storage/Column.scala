package edu.cmu.graphchidb.storage

import edu.cmu.graphchidb.{Util, GraphChiDatabase, DatabaseIndexing}
import edu.cmu.graphchidb.storage.ByteConverters._

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.io._
import java.sql.DriverManager
import edu.cmu.graphchi.preprocessing.VertexIdTranslate
import java.nio.{MappedByteBuffer, LongBuffer, ByteBuffer}
import scala.Some
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer

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
  val prefixFilename = filePrefix +  ".vardata_" + name + "_" + _pointerColumn.indexing.name

  var maxFileSize = 128 * 1024 * 1024 // 128 megs

  val pointerColumn = _pointerColumn

  private val bufferSize = 100000
  private var bufferIdStart = 0L
  private var bufferIndexStart = 0L
  private var bufferCounter = new AtomicInteger()

  private val buffer = new ByteArrayOutputStream(bufferSize * 100) {
    def currentBuf = buf
  }

  var initialized = false

  val bufferIndices = new Array[Long](bufferSize)
  val idSeq = new AtomicInteger()

  val lock = new ReentrantReadWriteLock()

  def partialFileName(id: Int) = prefixFilename + ".%d".format(id)
  def partialIdxFileName(id: Int) = partialFileName(id) + ".idx"

  case class PartialVarDataFile(id: Int, idxBuffer: MappedByteBuffer, dataBuffer: MappedByteBuffer)

  def initPartialData(id: Int) = {
    val idxFile = new File(partialIdxFileName(id))
    if (!idxFile.exists()) idxFile.createNewFile()
    val idxFileChannel = new RandomAccessFile(idxFile, "r").getChannel
    val idxBuffer = idxFileChannel.map(MapMode.READ_ONLY, 0, idxFile.length())
    idxFileChannel.close()
    val dataFile = new File(partialFileName(id))
    if (!dataFile.exists()) dataFile.createNewFile()
    val dataFileChannel = new RandomAccessFile(dataFile, "r").getChannel
    val dataBuffer = dataFileChannel.map(MapMode.READ_ONLY, 0, dataFile.length())
    dataFileChannel.close()
    PartialVarDataFile(id, idxBuffer, dataBuffer)
  }


  var currentBufferPartId = 0
  val partialDataFiles = new ArrayBuffer[PartialVarDataFile]()

  def init() {
    this.synchronized {
      if (!initialized) {

        val existing =  Stream.from(0) takeWhile (i => {
          val f = new File(partialFileName(i))
          f.exists()
        })
        partialDataFiles ++= existing.map(i => initPartialData(i))
        startNewPart()
        initialized = true
        println("Initialized %s, new part id = %d".format(prefixFilename, currentBufferPartId))
      }
    }
  }



  init()

  def startNewPart(): Unit = {
    lock.writeLock().lock()
    try {
      partialDataFiles += initPartialData(currentBufferPartId)
      currentBufferPartId = partialDataFiles.size

      idSeq.set(0)
    } finally {
       lock.writeLock.unlock()
    }
  }

  def flushBuffer() = {
      lock.writeLock().lock()
      try {
        val dataFile = new File(partialFileName(currentBufferPartId))
        val logOutput = new FileOutputStream(dataFile, true)
        logOutput.write(buffer.toByteArray)
        logOutput.close()

        val byteBuffer = ByteBuffer.allocate(bufferIndices.length * 8)
        val longBuffer = byteBuffer.asLongBuffer()
        longBuffer.put(bufferIndices)

        val flushedIndicesAsBytes = byteBuffer.array()
        val indexFile = new File(partialIdxFileName(currentBufferPartId))
        val indexOut = new FileOutputStream(indexFile, true)
        indexOut.write(flushedIndicesAsBytes, 0, bufferCounter.get * 8)


        partialDataFiles(partialDataFiles.size - 1) = initPartialData(partialDataFiles.last.id)

        println("flush: %s %d".format(prefixFilename, currentBufferPartId))

        bufferCounter.set(0)
        buffer.reset()

        if (dataFile.length > maxFileSize) {
          startNewPart()
          bufferIndexStart = 0
        } else {
          bufferIndexStart = partialDataFiles.last.dataBuffer.capacity()
        }
        bufferIdStart = idSeq.get

      } finally {
        lock.writeLock().unlock()
      }

  }

  def insert(data: Array[Byte]) : Long = {
    lock.writeLock().lock
    try {
      if (!initialized) throw new IllegalStateException("Not initialized")
      bufferIndices(bufferCounter.getAndIncrement) = bufferIndexStart + buffer.size()
      buffer.write(data)
      val id = idSeq.getAndIncrement
      val bufPartId = currentBufferPartId
      if (bufferCounter.get == bufferSize) {
        flushBuffer
      }
      Util.setHiLo(bufPartId, id)
    } finally {
      lock.writeLock().unlock()
    }
  }

  def get(globalId: Long) : Array[Byte] = {
    val partialId = Util.hiBytes(globalId)
    val localId = Util.loBytes(globalId)
    var needLock = partialId == currentBufferPartId
    if (needLock) lock.readLock().lock()
    try {
      if (needLock && localId >= bufferIdStart) {
        // Look from buffers
        val idOff = (localId - bufferIdStart).toInt
        val bufferOff = bufferIndices(idOff)
        val len = if (idOff < bufferCounter.get - 1) { bufferIndices(idOff + 1) - bufferOff} else
        { buffer.size() - (bufferOff - bufferIndexStart) }

        val res = new Array[Byte](len.toInt)
          Array.copy(buffer.currentBuf, (bufferOff - bufferIndexStart).toInt, res, 0, len.toInt)

        res
      } else {
        if (partialId >= partialDataFiles.size) {
           println("Accessing partial id %d, but size: %d, globalid=%s".format(partialId, partialDataFiles.size, globalId))
        }

        // Seek file
        val indexBuffer = partialDataFiles(partialId).idxBuffer
        val dataBuffer = partialDataFiles(partialId).dataBuffer
        val idxPos = localId * 8

        val fileOff = indexBuffer.getLong(idxPos)
        val next = if (idxPos < indexBuffer.capacity() - 8) { indexBuffer.getLong(idxPos + 8) } else
        { dataBuffer.capacity() }
        val len = next - fileOff

        if (len < 0) {
          printf("Error in size comp: %d %d %d %d %s %s\n".format(localId, next, indexBuffer.capacity(), fileOff,
            indexBuffer.getLong(idxPos.toInt), indexBuffer.getLong(idxPos.toInt + 8)))
        }

        val res = new Array[Byte](len.toInt)

        val tmpBuffer = dataBuffer.duplicate()
        tmpBuffer.position(fileOff.toInt)
        tmpBuffer.get(res)
        res
      }
    } finally {
      if (needLock)  lock.readLock().unlock()
    }
  }

  def delete(id: Long) : Unit = {
    // Not implemented now
  }
}

/*
           if (new File(logIdxFile).exists()) {
        idSeq.set(new File(logIdxFile).length / 8)
        bufferIdStart = idSeq.get
        bufferIndexStart = new File(logFileName).length()
        indexBuffer = indexFile.getChannel.map(MapMode.READ_ONLY, 0, indexFile.length())
        dataBuffer = logOutput.getChannel.map(MapMode.READ_ONLY, 0, logOutput.length())

      }
      println("Var data index start: " + bufferIdStart + " / " + bufferIndexStart)
      initialized = true

  def flushBuffer(): Unit = {
    lock.writeLock().lock()
    try {
    logOutput.seek(logOutput.length())
    logOutput.write(buffer.toByteArray)

    val byteBuffer = ByteBuffer.allocate(bufferIndices.length * 8)
    val longBuffer = byteBuffer.asLongBuffer()
    longBuffer.put(bufferIndices)

    val flushedIndicesAsBytes = byteBuffer.array()
    indexFile.seek(indexFile.length())
    indexFile.write(flushedIndicesAsBytes, 0, bufferCounter.get * 8)

    bufferCounter.set(0)
    bufferIndexStart = logOutput.length()
    bufferIdStart = idSeq.get
    buffer.reset()
    indexBuffer = indexFile.getChannel.map(MapMode.READ_ONLY, 0, indexFile.length())
    dataBuffer = logOutput.getChannel.map(MapMode.READ_ONLY, 0, logOutput.length())

    println("flush: " + new File(logFileName).getAbsolutePath + ", " +  new File(logFileName).length())
    } finally {
       lock.writeLock().unlock()
    }
  }

 def insert(data: Array[Byte]) : Long = {
    lock.writeLock().lock
    try {
      if (!initialized) throw new IllegalStateException("Not initialized")
      bufferIndices(bufferCounter.getAndIncrement) = bufferIndexStart + buffer.size()
      buffer.write(data)
      val id = idSeq.getAndIncrement

      if (bufferCounter.get == bufferSize) {
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
        val len = if (idOff < bufferCounter.get - 1) { bufferIndices(idOff + 1) - bufferOff} else
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
        val idxPos = id * 8
        val fileOff = indexBuffer.getLong((idxPos).toInt)
        val next = if (idxPos < indexBuffer.capacity() - 8) { indexBuffer.getLong((idxPos + 8).toInt) } else
            { dataBuffer.capacity() }
        val len = next - fileOff

        if (len < 0) {
          printf("Error in size comp: %d %d %d %d %s %s\n".format(id, next, indexBuffer.capacity(), fileOff,
            indexBuffer.getLong(idxPos.toInt), indexBuffer.getLong(idxPos.toInt + 8)))
        }

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
 */

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