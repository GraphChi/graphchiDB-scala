package edu.cmu.graphchidb.storage

import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
import edu.cmu.graphchidb.storage.ByteConverters._

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.io.File

/**
 * Database column
 * @author Aapo Kyrola
 */
class Column[T](filePrefix: String, sparse: Boolean, _indexing: DatabaseIndexing,
                  converter: ByteConverter[T]) {

  val indexing = _indexing
  def blockFilename(shardNum: Int) = filePrefix + "." + shardNum

  val blocks = (0 until indexing.shards).map {
    shard =>
      if (!sparse) {
        new  MemoryMappedDenseByteStorageBlock(new File(blockFilename(shard)), indexing.shardSize(shard),
          converter.sizeOf) with DataBlock[T]
      } else {
        throw new NotImplementedException()
      }
  }

  def get(idx: Long) =  blocks(indexing.shardForIndex(idx)).get(indexing.globalToLocal(idx).toInt)(converter)
  def getName(idx: Long) : String = get(idx).toString
  def set(idx: Long, value: T) : Unit = blocks(indexing.shardForIndex(idx)).set(indexing.globalToLocal(idx).toInt, value)(converter)

}

class CategoricalColumn(filePrefix: String, indexing: DatabaseIndexing, values: IndexedSeq[String])
  extends Column[Byte](filePrefix, sparse=false, indexing, ByteByteConverter) {

  def categoryName(i: Int) = values(i)
  def indexForName(name: String) = values.indexOf(name)

  def byteToInt(b: Byte) : Int = if (b < 0) 256 + b  else b

  def set(idx: Long, valueName: String) : Unit = super.set(idx, indexForName(valueName).toByte)
  override def getName(idx: Long) : String = categoryName(byteToInt(get(idx)))

}
