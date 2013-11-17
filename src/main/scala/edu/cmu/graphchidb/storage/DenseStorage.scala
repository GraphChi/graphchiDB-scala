package edu.cmu.graphchidb.storage

import java.io.{RandomAccessFile, FileOutputStream, File}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer

/**
 * @author Aapo Kyrola
 */
class MemoryMappedDenseByteStorageBlock(file: File, _size: Long, elementSize: Int) extends IndexedByteStorageBlock {

  /** If file exists and is of proper size, do nothing - otherwise initialize */
  if (!file.exists()) {
    val success = file.createNewFile()
    if (!success) throw new IllegalAccessException("Could not create file: " + file.getAbsolutePath)
  }
  // Ensure size
  private val expectedSize = elementSize.toLong * _size

  if (expectedSize > Int.MaxValue)
    throw new IllegalStateException("Data block size too big: " + expectedSize + ", file=" + file.getName)

  val currentSize = file.length()
  if (currentSize != expectedSize) {
    val fileChannel = new FileOutputStream(file, true).getChannel
    fileChannel.truncate(expectedSize)
    fileChannel.close()
  }

  var byteBuffer = {
    val channel = new RandomAccessFile(file, "rw").getChannel
    val bb = channel.map(FileChannel.MapMode.READ_WRITE, 0, expectedSize)
    channel.close
    bb
  }


  def valueLength = elementSize

  def readIntoBuffer(idx: Int, out: ByteBuffer) = {
      byteBuffer.position(idx * elementSize)
      byteBuffer.get(out.array())
      true
  }

  def writeFromBuffer(idx: Int, in: ByteBuffer) = {
    byteBuffer.position(idx * elementSize)
    byteBuffer.put(in.array())
  }

  def size = byteBuffer.capacity() / elementSize

  /* Creates replacement block with new data. Previous one is invalid after this.
   * TODO: questionable design.
   */
  def createNew[T](data: Array[Byte]) : MemoryMappedDenseByteStorageBlock with DataBlock[T] = {
     byteBuffer = null
     file.delete() // Delete file     // TODO: think if better way
     val newBlock = new MemoryMappedDenseByteStorageBlock(file, data.size / elementSize, elementSize) with DataBlock[T]
     /* Set data */
     newBlock.byteBuffer.put(data)
     newBlock
  }
}
