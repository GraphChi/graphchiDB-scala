package edu.cmu.graphchidb.storage

import edu.cmu.graphchi.preprocessing.VertexIdTranslate
import edu.cmu.graphchidb.{DatabaseIndexing, Util}
import java.io.{FileOutputStream, RandomAccessFile, File, ByteArrayOutputStream}
import java.nio.channels.FileChannel.MapMode
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/** Vardata columns have two parts: one is long-column holding indices to the
  * var data payload, which is stored in a special log (TODO: garbage collection).
  */
class VarDataColumn(name: String,  filePrefix: String, indexing: DatabaseIndexing)   {
  val prefixFilename = filePrefix +  ".vardata_" + name + "_" + indexing.name

  var maxFileSize = 128 * 1024 * 1024 // 128 megs


  private val bufferSize = 100000
  private var bufferIdStart = 0L
  private var bufferIndexStart = 0L
  private var bufferCounter = new AtomicInteger()

  private val buffer = new ByteArrayOutputStream(bufferSize * 100) {
    def currentBuf = buf
  }

  var initialized = false

  val bufferIndices = new Array[Long](bufferSize)
  val idSeq = new AtomicInteger()

  val lock = new ReentrantReadWriteLock()

  def partialFileName(id: Int) = prefixFilename + ".%d".format(id)
  def partialIdxFileName(id: Int) = partialFileName(id) + ".idx"

  case class PartialVarDataFile(id: Int, idxBuffer: MappedByteBuffer, dataBuffer: MappedByteBuffer)

  def initPartialData(id: Int) = {
    val idxFile = new File(partialIdxFileName(id))
    if (!idxFile.exists()) idxFile.createNewFile()
    val idxFileChannel = new RandomAccessFile(idxFile, "r").getChannel
    val idxBuffer = idxFileChannel.map(MapMode.READ_ONLY, 0, idxFile.length())
    idxFileChannel.close()
    val dataFile = new File(partialFileName(id))
    if (!dataFile.exists()) dataFile.createNewFile()
    val dataFileChannel = new RandomAccessFile(dataFile, "r").getChannel
    val dataBuffer = dataFileChannel.map(MapMode.READ_ONLY, 0, dataFile.length())
    dataFileChannel.close()
    PartialVarDataFile(id, idxBuffer, dataBuffer)
  }


  var currentBufferPartId = 0
  val partialDataFiles = new ArrayBuffer[PartialVarDataFile]()

  def init() {
    this.synchronized {
      if (!initialized) {

        val existing =  Stream.from(0) takeWhile (i => {
          val f = new File(partialFileName(i))
          f.exists()
        })
        partialDataFiles ++= existing.map(i => initPartialData(i))
        startNewPart()
        initialized = true
        println("Initialized %s, %d, new part id = %d".format(prefixFilename, partialDataFiles.size, currentBufferPartId))
      }
    }
  }



  init()

  def startNewPart(): Unit = {
    lock.writeLock().lock()
    try {
      val newId = if (partialDataFiles.isEmpty) { 0 } else {partialDataFiles.last.id + 1 }
      partialDataFiles += initPartialData(newId)
      currentBufferPartId = newId
      idSeq.set(0)
    } finally {
      lock.writeLock.unlock()
    }
  }

  def flushBuffer() = {
    lock.writeLock().lock()
    try {
      val dataFile = new File(partialFileName(currentBufferPartId))
      val logOutput = new FileOutputStream(dataFile, true)
      logOutput.write(buffer.toByteArray)
      logOutput.close()

      val byteBuffer = ByteBuffer.allocate(bufferIndices.length * 8)
      val longBuffer = byteBuffer.asLongBuffer()
      longBuffer.put(bufferIndices)

      val flushedIndicesAsBytes = byteBuffer.array()
      val indexFile = new File(partialIdxFileName(currentBufferPartId))
      val indexOut = new FileOutputStream(indexFile, true)
      indexOut.write(flushedIndicesAsBytes, 0, bufferCounter.get * 8)


      partialDataFiles(partialDataFiles.size - 1) = initPartialData(partialDataFiles.last.id)

      println("flush: %s %d".format(prefixFilename, currentBufferPartId))

      bufferCounter.set(0)
      buffer.reset()

      if (dataFile.length > maxFileSize) {
        startNewPart()
        bufferIndexStart = 0
      } else {
        bufferIndexStart = partialDataFiles.last.dataBuffer.capacity()
      }
      bufferIdStart = idSeq.get

    } finally {
      lock.writeLock().unlock()
    }

  }

  def insert(data: Array[Byte]) : Long = {
    lock.writeLock().lock
    try {
      if (!initialized) throw new IllegalStateException("Not initialized")
      bufferIndices(bufferCounter.getAndIncrement) = bufferIndexStart + buffer.size()
      buffer.write(data)
      val id = idSeq.getAndIncrement
      val bufPartId = currentBufferPartId
      if (bufferCounter.get == bufferSize) {
        flushBuffer
      }
      Util.setHiLo(bufPartId, id)
    } finally {
      lock.writeLock().unlock()
    }
  }

  def get(globalId: Long) : Array[Byte] = {
    val partialId = Util.hiBytes(globalId)
    val localId = Util.loBytes(globalId)
    var needLock = partialId == currentBufferPartId
    if (needLock) lock.readLock().lock()
    try {
      if (needLock && localId >= bufferIdStart) {
        // Look from buffers
        val idOff = (localId - bufferIdStart).toInt
        val bufferOff = bufferIndices(idOff)
        val len = if (idOff < bufferCounter.get - 1) { bufferIndices(idOff + 1) - bufferOff} else
        { buffer.size() - (bufferOff - bufferIndexStart) }

        val res = new Array[Byte](len.toInt)
        Array.copy(buffer.currentBuf, (bufferOff - bufferIndexStart).toInt, res, 0, len.toInt)

        res
      } else {
        if (partialId >= partialDataFiles.size) {
          println("Accessing partial id %d, but size: %d, globalid=%s".format(partialId, partialDataFiles.size, globalId))
        }

        // Seek file
        val indexBuffer = partialDataFiles(partialId).idxBuffer
        val dataBuffer = partialDataFiles(partialId).dataBuffer
        val idxPos = localId * 8

        if (idxPos > indexBuffer.capacity()) {
          println("Illegal index %d / %d".format(idxPos, indexBuffer.capacity()))
        }
        val fileOff = indexBuffer.getLong(idxPos)
        val next = if (idxPos < indexBuffer.capacity() - 8) { indexBuffer.getLong(idxPos + 8) } else
        { dataBuffer.capacity() }
        val len = next - fileOff

        if (len < 0) {
          printf("Error in size comp: %d %d %d %d %s %s\n".format(localId, next, indexBuffer.capacity(), fileOff,
            indexBuffer.getLong(idxPos.toInt), indexBuffer.getLong(idxPos.toInt + 8)))
        }

        val res = new Array[Byte](len.toInt)

        val tmpBuffer = dataBuffer.duplicate()
        if (fileOff < 0 || fileOff > tmpBuffer.capacity()) {
          println("Illegal %d %d".format(fileOff, tmpBuffer.capacity()))
        }
        tmpBuffer.position(fileOff.toInt)
        tmpBuffer.get(res)
        res
      }
    } finally {
      if (needLock)  lock.readLock().unlock()
    }
  }

  def delete(id: Long) : Unit = {
    // Not implemented now
  }
}
