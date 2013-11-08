package edu.cmu.graphchidb.storage

import java.nio.ByteBuffer
import java.io.File

/**
 * DataBlock is a low level storage object, that stores key-value pairs.
 * @author Aapo Kyrola
 */

trait ByteConverter[T] {
  def fromBytes(bb: ByteBuffer) : T
  def toBytes(v: T) : ByteBuffer
  def sizeOf: Int
}

object ByteConverters {
  implicit object IntByteConverter extends ByteConverter[Int] {
    override def fromBytes(bb: ByteBuffer) : Int = {
      bb.getInt
    }
    override def toBytes(v: Int) : ByteBuffer = {
      val bb = ByteBuffer.allocate(4)
      bb.putInt(v)
      bb
    }
    override def sizeOf = 4
  }

  implicit  object ByteByteConverter extends ByteConverter[Byte] {
    override def fromBytes(bb: ByteBuffer) : Byte = {
      bb.get
    }
    override def toBytes(v: Byte) : ByteBuffer = {
      val bb = ByteBuffer.allocate(1)
      bb.put(v)
      bb
    }
    override def sizeOf = 1
  }
}

trait DataBlock[T] extends IndexedByteStorageBlock {

  def get(idx: Int)(implicit converter: ByteConverter[T]) : Option[T] = {
    val byteBuffer = ByteBuffer.wrap(new Array[Byte](valueLength))
    byteBuffer.rewind()
    if (readIntoBuffer(idx, byteBuffer))
      Some(converter.fromBytes(byteBuffer))
    else
      None
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
  def readIntoBuffer(idx: Int, out: ByteBuffer) : Boolean
  def writeFromBuffer(idx: Int, in: ByteBuffer) : Unit

}
