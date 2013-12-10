package edu.cmu.graphchidb.storage

import java.nio.ByteBuffer
import java.io.File

/**
 * DataBlock is a low level storage object, that stores key-value pairs.
 * @author Aapo Kyrola
 */

trait ByteConverter[T] {
  def fromBytes(bb: ByteBuffer) : T
  def toBytes(v: T, out: ByteBuffer) : Unit
  def sizeOf: Int
}

object ByteConverters {
  implicit object IntByteConverter extends ByteConverter[Int] {
    override def fromBytes(bb: ByteBuffer) : Int = {
      bb.getInt
    }
    override def toBytes(v: Int, bb: ByteBuffer) : Unit = {
      bb.putInt(v)
    }
    override def sizeOf = 4
  }

  implicit object ShortByteConverter extends ByteConverter[Short] {
    override def fromBytes(bb: ByteBuffer) : Short = {
      bb.getShort
    }
    override def toBytes(v: Short, bb: ByteBuffer) : Unit = {
      bb.putShort(v)
    }
    override def sizeOf = 2
  }

  implicit object FloatByteConverter extends ByteConverter[Float] {
    override def fromBytes(bb: ByteBuffer) : Float = {
      bb.getFloat
    }
    override def toBytes(v: Float, bb: ByteBuffer) : Unit = {
      bb.putFloat(v)
    }
    override def sizeOf = 4
  }

  implicit object LongByteConverter extends ByteConverter[Long] {
    override def fromBytes(bb: ByteBuffer) : Long = {
      bb.getLong
    }
    override def toBytes(v: Long, bb: ByteBuffer) : Unit = {
      bb.putLong(v)
    }
    override def sizeOf = 8
  }

  implicit  object ByteByteConverter extends ByteConverter[Byte] {
    override def fromBytes(bb: ByteBuffer) : Byte = {
      bb.get
    }
    override def toBytes(v: Byte, bb: ByteBuffer) : Unit = {
      bb.put(v)
    }
    override def sizeOf = 1
  }

}

trait DataBlock[T] extends IndexedByteStorageBlock {



  def get(idx: Int, byteBuffer: ByteBuffer)(implicit converter: ByteConverter[T]) : Option[T] = {
    if (readIntoBuffer(idx, byteBuffer)) {
      byteBuffer.rewind()
      Some(converter.fromBytes(byteBuffer))
    } else
      None
  }

  def get(idx: Int)(implicit converter: ByteConverter[T]) : Option[T] = {
    val byteBuffer = ByteBuffer.allocate(valueLength)
    get(idx, byteBuffer)(converter)
  }

  def set(idx: Int, value: T, bb: ByteBuffer)(implicit converter: ByteConverter[T]) : Unit = {
    converter.toBytes(value, bb)
    bb.rewind()
    writeFromBuffer(idx, bb)
  }

  def set(idx: Int, value: T)(implicit converter: ByteConverter[T]) : Unit = {
    val bb = ByteBuffer.allocate(converter.sizeOf)  // TODO reuse
    set(idx, value, bb)
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
