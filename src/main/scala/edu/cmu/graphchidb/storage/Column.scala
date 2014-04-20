/**
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2014] [Aapo Kyrola / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Publication to cite:  http://arxiv.org/abs/1403.0701
 */
package edu.cmu.graphchidb.storage

import edu.cmu.graphchidb.{Util, DatabaseIndexing}
import edu.cmu.graphchidb.storage.ByteConverters._

import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.io._
import java.nio.{ByteBuffer}


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

  def updateAll(updateFunc: (Long, Option[T]) => T) : Unit

  def indexing: DatabaseIndexing

  def readValueBytes(shardNum: Int, idx: Int, buf: ByteBuffer) : Unit

  def recreateWithData(shardNum: Int, data: Array[Byte]) : Unit

  def foldLeft[B](z: B)(op: (B, T, Long) => B): B
  def foreach(op: (Long, T) => Unit) : Unit

  def select(op: (Long, T) => Boolean) : Iterator[(Long, T)]

  def delete : Unit

  def typeInfo: String = ""

  // Bit ugly
  def autoFillVertex : Option[(Long) => T]
  def autoFillEdge : Option[(Long, Long, Byte) => T]

}

class FileColumn[T](id: Int, filePrefix: String, sparse: Boolean, _indexing: DatabaseIndexing,
                    converter: ByteConverter[T], deleteOnExit:Boolean=false) extends Column[T] {

  override def columnId = id

  /* Autofill functions (used mainly for computation) */
  var autoFillVertexFunc : Option[(Long) => T] = None
  var autoFillEdgeFunc : Option[(Long, Long, Byte) => T] = None
  override def autoFillVertex = autoFillVertexFunc
  override def autoFillEdge = autoFillEdgeFunc

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

        if (!_indexing.allowAutoExpansion) {
          val expectedSize = indexing.shardSize(shard) * converter.sizeOf
          if (!f.exists() || f.length() < expectedSize) {
            Util.initializeFile(f, expectedSize.toInt)
          }
        }

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
      this.synchronized {
        val EXPANSION_BLOCK = 4096 // TODO -- not right place
        val expansionSize = (scala.math.ceil((localIdx + 1).toDouble /  EXPANSION_BLOCK) * EXPANSION_BLOCK).toInt
        block.expand(expansionSize)
      }
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

  def foreach(op: (Long, T) => Unit) : Unit = {
    (0 until blocks.size).foreach(shardIdx => blocks(shardIdx).foreach((localIdx:Long, v: T)
    => op(indexing.localToGlobal(shardIdx, localIdx), v))(converter))
  }

  def updateAll(updateFunc: (Long, Option[T]) => T) : Unit = {
    (0 until blocks.size).foreach(shardIdx => blocks(shardIdx).updateAll((localIdx:Long, v: Option[T])
    => updateFunc(indexing.localToGlobal(shardIdx, localIdx), v))(converter))
  }


  def fillWithZeroes = {
    blocks.foreach(b => b.fillWithZeroes)
  }


  def select(cond: (Long, T) => Boolean) : Iterator[(Long, T)]  = {

    // Probably easier ways to do this exist....
    new Iterator[(Long, T)] {
      var shardIdx = 0

      def itForShard(s: Int) =  blocks(s).select((localIdx:Long, v: T) =>
        cond(indexing.localToGlobal(s, localIdx), v))(converter)

      var currentIterator = itForShard(0)

      def hasNext = if (currentIterator.hasNext) { true } else {
        shardIdx += 1
        if (shardIdx >= blocks.size) {
          false
        } else {
          currentIterator = itForShard(shardIdx)
          hasNext
        }
      }

      def next() = {
        val (localIdx, v) = currentIterator.next()
        (indexing.localToGlobal(shardIdx, localIdx), v)
      }
    }

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

