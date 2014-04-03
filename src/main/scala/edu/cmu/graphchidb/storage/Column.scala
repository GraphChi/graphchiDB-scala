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
  def getMany(idxs: Set[Long]) : Map[Long, Option[T]] = {
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

  def foldLeft[B](z: B)(op: (B, T, Long) => B): B

  def delete : Unit

  def typeInfo: String = ""

}

class FileColumn[T](id: Int, filePrefix: String, sparse: Boolean, _indexing: DatabaseIndexing,
                    converter: ByteConverter[T], deleteOnExit:Boolean=false) extends Column[T] {

  override def columnId = id

  def encode(value: T, out: ByteBuffer) = converter.toBytes(value, out)
  def decode(in: ByteBuffer) = converter.fromBytes(in)
  def elementSize = converter.sizeOf

  override def typeInfo = converter.getClass.getName

  def indexing = _indexing
  def blockFilename(shardNum: Int) = filePrefix + "." + shardNum


  var blocks = (0 until indexing.nShards).map {
    shard =>
      if (!sparse) {
        val f = new File(blockFilename(shard))
        if (deleteOnExit) f.deleteOnExit()
        new  MemoryMappedDenseByteStorageBlock(f, None,
          converter.sizeOf) with DataBlock[T]
      } else {
        throw new NotImplementedException()
      }
  }.toArray

  def delete = blocks.foreach(b => b.delete)

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

  def getOrElse[T](idx:Long, default: T) = get(idx).getOrElse(default)

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

  def foldLeft[B](z: B)(op: (B, T, Long) => B): B = {
    (0 until blocks.size).foldLeft(z){ case (cum: B, shardIdx: Int) => blocks(shardIdx).foldLeft(cum)(
      {
        case (cum: B, x: T, localIdx: Int) => op(cum, x, indexing.localToGlobal(shardIdx, localIdx))
      })(converter) }
  }

}

class CategoricalColumn(id: Int, filePrefix: String, indexing: DatabaseIndexing, values: IndexedSeq[String],
                         deleteOnExit: Boolean = false)
  extends FileColumn[Byte](id, filePrefix, sparse=false, indexing, ByteByteConverter, deleteOnExit) {

  def byteToInt(b: Byte) : Int = if (b < 0) 256 + b  else b

  def categoryName(b: Byte) : String = values(byteToInt(b))
  def categoryName(i: Int)  : String = values(i)
  def indexForName(name: String) = values.indexOf(name).toByte


  def set(idx: Long, valueName: String) : Unit = super.set(idx, indexForName(valueName).toByte)
  override def getName(idx: Long) : Option[String] = get(idx).map( vidx => categoryName(byteToInt(vidx))).headOption

}


class MySQLBackedColumn[T](id: Int, tableName: String, columnName: String, _indexing: DatabaseIndexing,
                           vertexIdTranslate: VertexIdTranslate) extends Column[T] {

  def encode(value: T, out: ByteBuffer) = throw new UnsupportedOperationException
  def decode(in: ByteBuffer) = throw new UnsupportedOperationException
  def elementSize = throw new UnsupportedOperationException

  override def columnId = id

  override  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit = throw new UnsupportedOperationException

  def delete = throw new NotImplementedException


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


  override def getMany(_idxs: Set[Long]) : Map[Long, Option[T]] = {
    val idxs = new Array[Long](_idxs.size)
    _idxs.zipWithIndex.foreach { tp=> { idxs(tp._2) = vertexIdTranslate.backward(tp._1) } }

    val pstmt = dbConnection.prepareStatement("select id, %s from %s where id in (%s)".format(columnName, tableName,
      idxs.mkString(",")))
    try {
      val rs = pstmt.executeQuery()

      new Iterator[(Long, Option[T])] {
        def hasNext = rs.next()
        def next() = (vertexIdTranslate.forward(rs.getLong(1)), Some(rs.getObject(2).asInstanceOf[T]))
      }.toStream.toMap

    } finally {
      if (pstmt != null) pstmt.close()
    }


  }
  def foldLeft[B](z: B)(op: (B, T, Long) => B): B = {
      throw new NotImplementedException
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