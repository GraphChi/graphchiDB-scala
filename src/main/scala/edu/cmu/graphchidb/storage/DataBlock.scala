package edu.cmu.graphchidb.storage

import java.nio.ByteBuffer

/**
 * DataBlock is a low level storage object, that stores key-value pairs.
 * @author Aapo Kyrola
 */

trait ByteConverter[T] {
  def fromBytes(bb: ByteBuffer) : T
  def toBytes(v: T) : ByteBuffer
}

object ByteConverters {
  implicit object IntByteConverter extends ByteConverter[Int] {
    def fromBytes(bb: ByteBuffer) : Int = {
      bb.getInt
    }
    def toBytes(v: Int) : ByteBuffer = {
      val bb = ByteBuffer.wrap(new Array[Byte](4))
      bb.putInt(v)
      bb
    }

  }
}

trait DataBlock[T] extends IndexedByteStorageBlock {


  def get(idx: Int)(implicit converter: ByteConverter[T]) : T = {
    val byteBuffer = ByteBuffer.wrap(new Array[Byte](valueLength))
    byteBuffer.rewind()
    readIntoBuffer(idx, byteBuffer)
    converter.fromBytes(byteBuffer)
  }

  def set(idx: Int, value: T)(implicit converter: ByteConverter[T]) : Unit = {
    writeFromBuffer(idx, converter.toBytes(value))
  }

}

/*
 * Internal low-level
 */
trait IndexedByteStorageBlock  {

  def valueLength: Int
  def readIntoBuffer(idx: Int, out: ByteBuffer) : Unit
  def writeFromBuffer(idx: Int, in: ByteBuffer) : Unit

}

trait CategoricalDataBlock extends DataBlock[Byte] {
  def getName(indexedId: Byte) : String

}
