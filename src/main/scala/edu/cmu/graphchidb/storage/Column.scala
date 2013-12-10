package edu.cmu.graphchidb.storage

import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
import edu.cmu.graphchidb.storage.ByteConverters._

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.io.{ByteArrayInputStream, File}
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
  * var data payload, which is stored separately (currently in MySQL).
  */
class VarDataColumn(name: String,  filePrefix: String, _pointerColumn: Column[Long])   {
  val tableName =  "vardata_" + math.abs(filePrefix.hashCode) + "_" + name + "_" + _pointerColumn.indexing.name

  println(tableName)

  def pointerColumn = _pointerColumn

  val dbConnection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://127.0.0.1/graphchidb?" +
      "user=graphchidb&password=dbchi9999")
  }

  def initMySQLBacking() = {
    val stmt = dbConnection.createStatement()
    try {
      val rset = stmt.executeQuery("select * from %s".format(tableName))
      // Table exists because no exception
      rset.close()
    } catch {
      case e : Exception =>  {
        // Create table
        try {
          stmt.execute("CREATE TABLE %s (id int auto_increment primary key,  payload mediumblob)")
        } catch {case e: Exception => e.printStackTrace()}
      }
    } finally {
      try { stmt.close() } catch {case e: Exception => e.printStackTrace()}
    }
  }

  initMySQLBacking()

  def insert(data: Array[Byte]) : Long = {
    this.synchronized {  // TODO, remove sync!
    var pstmt = dbConnection.prepareStatement("insert into %s (payload) values (?)".format(tableName))
      try {

        pstmt.setBlob(1, new ByteArrayInputStream(data))
        pstmt.executeUpdate()
        pstmt.close()

        pstmt = dbConnection.prepareStatement("select last_insert_id()")
        val rset = pstmt.executeQuery()

        return rset.getLong(1)
      } catch {
        case e: Exception => e.printStackTrace()

      } finally {
        try { pstmt.close() } catch {case e: Exception => e.printStackTrace()}
      }
      throw new RuntimeException("Could not insert payload!")
    }
  }

  def get(id: Long) : Array[Byte] = {
    var pstmt = dbConnection.prepareStatement("select payload from %s where id=?".format(tableName))
    try {
      pstmt.setLong(1, id)
      val rset = pstmt.executeQuery()
      val dataBlob = rset.getBlob(1)
      return dataBlob.getBytes(0, dataBlob.length().toInt)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      try { pstmt.close() } catch {case e: Exception => e.printStackTrace()}
    }
    throw new RuntimeException("Could not get payload for id=%s!".format(id))

  }

  def delete(id: Long) : Unit = {
    var pstmt = dbConnection.prepareStatement("delete from %s where id=?".format(tableName))
    try {
      pstmt.setLong(1, id)
      pstmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      try { pstmt.close() } catch {case e: Exception => e.printStackTrace()}
    }
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