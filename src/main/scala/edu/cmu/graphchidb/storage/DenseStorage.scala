package edu.cmu.graphchidb.storage

import java.io.{RandomAccessFile, FileOutputStream, File}
import java.nio.channels.FileChannel
import java.nio.{BufferUnderflowException, ByteBuffer}

/**
 * @author Aapo Kyrola
 */
class MemoryMappedDenseByteStorageBlock(file: File, _size: Option[Long], elementSize: Int, truncate : Boolean = false) extends IndexedByteStorageBlock {

  /** If file exists and is of proper size, do nothing - otherwise initialize */
  if (!file.exists()) {
    val success = file.createNewFile()
    if (!success) throw new IllegalAccessException("Could not create file: " + file.getAbsolutePath)
  }
  // Ensure size
  private val initialSize = if (_size.isDefined) { elementSize.toLong * _size.get } else { file.length() }

  if (initialSize > Int.MaxValue)
    throw new IllegalStateException("Data block size too big: " + initialSize + ", file=" + file.getName)

  val currentSize = file.length()
  if (currentSize != initialSize) {
    // println("Shard size was not as expected: %s, %d != %d".format(file.getName, currentSize, initialSize))
    if (truncate) {
      val channel = new FileOutputStream(file, true).getChannel
      channel.truncate(initialSize)
      channel.close()
    }
  }

  def delete() : Unit = {
    byteBuffer = null
    file.delete()
  }

  private def mmap(size: Long) =
  {
    val channel = new RandomAccessFile(file, "rw").getChannel
    val bb = channel.map(FileChannel.MapMode.READ_WRITE, 0, size)
    channel.close
    bb
  }

  var byteBuffer = mmap(initialSize)

  def expand(newSizeInElements: Int) = {
    this.synchronized {
      val targetSize = newSizeInElements * elementSize
      var newBytes = targetSize - file.length()
      val out = new FileOutputStream(file, true)
      while(newBytes > 0) {
        val len = scala.math.min(16384, newBytes).toInt
        val bb = new Array[Byte](len)
        out.write(bb)
        newBytes -= len
      }
      out.close()
      assert(file.length() == targetSize)

      // Reload memory mapped byte buffer
      byteBuffer = mmap(targetSize)
    }
  }

  def valueLength = elementSize

  def readIntoBuffer(idx: Int, out: ByteBuffer) = {
    this.synchronized {
      byteBuffer.position(idx * elementSize)
      var i = 0
      while (i < elementSize) { out.put(byteBuffer.get()); i+=1}
      true
    }
  }

  def writeFromBuffer(idx: Int, in: ByteBuffer) = {
    this.synchronized {
      byteBuffer.position(idx * elementSize)
      byteBuffer.put(in.array())
    }
  }

  def size = byteBuffer.capacity() / elementSize

  /* Creates replacement block with new data. Previous one is invalid after this.
   * TODO: questionable design.
   */
  def createNew[T](data: Array[Byte]) : MemoryMappedDenseByteStorageBlock with DataBlock[T] = {
    byteBuffer = null
    file.delete() // Delete file     // TODO: think if better way
    val newBlock = new MemoryMappedDenseByteStorageBlock(file, Some(data.size / elementSize), elementSize, truncate=true) with DataBlock[T]
    /* Set data */
    newBlock.byteBuffer.put(data)
    newBlock
  }
}
