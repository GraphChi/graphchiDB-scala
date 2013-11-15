package edu.cmu.graphchidb.storage

import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
import edu.cmu.graphchidb.storage.ByteConverters._

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.io.File
import java.sql.DriverManager
import edu.cmu.graphchi.preprocessing.VertexIdTranslate
import java.nio.ByteBuffer

/**
 *
 *
 * Database column
 * @author Aapo Kyrola
 */

trait Column[T] {

  def encode(value: T, out: ByteBuffer) : Unit
  def decode(in: ByteBuffer) : T
  def elementSize: Int

  def get(idx: Long) : Option[T]
  def getMany(idxs: Set[java.lang.Long]) : Map[java.lang.Long, Option[T]] = {
    idxs.map(idx => idx -> get(idx)).toMap
  }
  def getName(idx: Long) : Option[String]
  def set(idx: Long, value: T) : Unit
  def indexing: DatabaseIndexing

  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit

  def recreateWithData(shardNum: Int, data: Array[Byte]) : Unit

}

class FileColumn[T](filePrefix: String, sparse: Boolean, _indexing: DatabaseIndexing,
                    converter: ByteConverter[T]) extends Column[T] {


  def encode(value: T, out: ByteBuffer) = converter.toBytes(value, out)
  def decode(in: ByteBuffer) = converter.fromBytes(in)
  def elementSize = converter.sizeOf

  def indexing = _indexing
  def blockFilename(shardNum: Int) = filePrefix + "." + shardNum


  var blocks = (0 until indexing.nShards).map {
      shard =>
        if (!sparse) {
          new  MemoryMappedDenseByteStorageBlock(new File(blockFilename(shard)), indexing.shardSize(shard),
            converter.sizeOf) with DataBlock[T]
        } else {
          throw new NotImplementedException()
        }
    }.toArray


  /* Internal call */
  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit = blocks(shardNum).readIntoBuffer(idx, buf)
  def get(idx: Long) =  blocks(indexing.shardForIndex(idx)).get(indexing.globalToLocal(idx).toInt)(converter)
  def getName(idx: Long) = get(idx).map(a => a.toString).headOption

  def set(idx: Long, value: T) : Unit = blocks(indexing.shardForIndex(idx)).set(indexing.globalToLocal(idx).toInt, value)(converter)

  /* Create data from scratch */
  def recreateWithData(shardNum: Int, data: Array[Byte]) : Unit = {
    blocks(shardNum) = blocks(shardNum).createNew[T](data)
  }
}

class CategoricalColumn(filePrefix: String, indexing: DatabaseIndexing, values: IndexedSeq[String])
  extends FileColumn[Byte](filePrefix, sparse=false, indexing, ByteByteConverter) {

  def byteToInt(b: Byte) : Int = if (b < 0) 256 + b  else b

  def categoryName(b: Byte) : String = values(byteToInt(b))
  def categoryName(i: Int)  : String = values(i)
  def indexForName(name: String) = values.indexOf(name).toByte


  def set(idx: Long, valueName: String) : Unit = super.set(idx, indexForName(valueName).toByte)
  override def getName(idx: Long) : Option[String] = get(idx).map( vidx => categoryName(byteToInt(vidx))).headOption

}

class MySQLBackedColumn[T](tableName: String, columnName: String, _indexing: DatabaseIndexing,
                           vertexIdTranslate: VertexIdTranslate) extends Column[T] {

  def encode(value: T, out: ByteBuffer) = throw new UnsupportedOperationException
  def decode(in: ByteBuffer) = throw new UnsupportedOperationException
  def elementSize = throw new UnsupportedOperationException


  override  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit = throw new UnsupportedOperationException

  // TODO: temporary code
  val dbConnection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://multi6.aladdin.cs.cmu.edu/graphchidb_aux?" +
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