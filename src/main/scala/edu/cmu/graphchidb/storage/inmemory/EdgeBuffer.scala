package edu.cmu.graphchidb.storage.inmemory

import edu.cmu.graphchidb.{DecodedEdge, EdgeEncoderDecoder}
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import edu.cmu.graphchi.queries.QueryCallback
import java.util
import edu.cmu.graphchi.shards.{PointerUtil, EdgeIterator}
import edu.cmu.graphchi.preprocessing.VertexIdTranslate

/**
 * Memory-efficient, but unsorted, in-memory edge buffer allowing for fast searches.
 * EdgeBuffer is bound to a specific edge encoding and decoding object.
 * Note: not thread-safe.
 * @author Aapo Kyrola
 */
class EdgeBuffer(encoderDecoder : EdgeEncoderDecoder, initialCapacityNumEdges: Int = 1024, bufferId: Int) {

  val edgeSize = encoderDecoder.edgeSize
  val tmpBuffer = ByteBuffer.allocate(edgeSize)

  var counter : Int = 0
  var deletedEdges : Int = 0
  var srcArray = new Array[Long](initialCapacityNumEdges)
  var dstArrayWithType = new Array[Long](initialCapacityNumEdges)

  /* Create special byteArrayOutStream that allows direct access */
  private val buffer = new ByteArrayOutputStream(initialCapacityNumEdges * encoderDecoder.edgeSize) {
    def currentBuf = buf
    def compact : Unit = {
      val newCurrentBuf = new Array[Byte](counter * encoderDecoder.edgeSize)
      Array.copy(currentBuf, 0, newCurrentBuf, 0, newCurrentBuf.length)
      buf = newCurrentBuf
    }
  }


  def encode(edgeType: Byte, vertexId: Long) = VertexIdTranslate.encodeVertexPacket(edgeType, vertexId, 0)
  def extractVertexId(x: Long) = VertexIdTranslate.getVertexId(x)
  def extractType(x: Long) =  VertexIdTranslate.getType(x)

  def addEdge(edgeType: Byte, src: Long, dst: Long, valueBytes: Array[Byte]) : Unit = {
    // Expand array. Maybe better to use scala's buffers, but not sure if they have the
    // optimization for primitives.
    if (counter == srcArray.size) {
      var newSrcArray = new Array[Long](counter * 2)
      var newDstArray = new Array[Long](counter * 2)
      Array.copy(srcArray, 0, newSrcArray, 0, srcArray.size)
      Array.copy(dstArrayWithType, 0, newDstArray, 0, dstArrayWithType.size)
      srcArray = newSrcArray
      dstArrayWithType = newDstArray
    }
    srcArray(counter) = src
    dstArrayWithType(counter) = encode(edgeType, dst)
    counter += 1
    buffer.write(valueBytes)
  }

  def addEdge(edgeType: Byte, src: Long, dst: Long, values: Any*) : Unit  = {
    tmpBuffer.rewind()
    encoderDecoder.encode(tmpBuffer, values:_*)
    addEdge(edgeType, src, dst, tmpBuffer.array())
  }

  private def edgeAtPos(idx: Int) = {
    val buf = ByteBuffer.wrap(buffer.currentBuf, idx * edgeSize, edgeSize)
    encoderDecoder.decode(buf, srcArray(idx), extractVertexId(dstArrayWithType(idx)))
  }

  // Sets a tombstone at edge.
  // TODO: think if should be refleced in numedges
  def deleteEdgeAt(idx: Int) = {
    if (extractType(dstArrayWithType(idx)) != VertexIdTranslate.DELETED_TYPE) {
      dstArrayWithType(idx) = encode(VertexIdTranslate.DELETED_TYPE, extractVertexId(dstArrayWithType(idx)))
      deletedEdges += 1
    }
  }

  def deleteAllEdgesForVertex(vertexId: Long) = {
    var i = 0
    while(i < counter) {
      if ((srcArray(i) == vertexId) || (extractVertexId(dstArrayWithType(i)) == vertexId)) {
        deleteEdgeAt(i)
      }
      i += 1
    }
  }

  def readEdgeIntoBuffer(idx: Int, buf: ByteBuffer) : Unit = {
    buf.put(buffer.currentBuf, idx * edgeSize, edgeSize)
  }

  def setColumnValue[T](idx: Int, columnIdx: Int, value: T) = {
    val buf = ByteBuffer.wrap(buffer.currentBuf, idx * edgeSize, edgeSize)
    encoderDecoder.setIthColumnInBuffer(buf, columnIdx, value)
  }

  /**
   * Sets the arrays to the size of the buffer
   */
  def compact : EdgeBuffer = {
    if (counter != srcArray.length) {
      val newSrcArray = new Array[Long](counter)
      val newDstArray = new Array[Long](counter)
      Array.copy(srcArray, 0, newSrcArray, 0, counter)
      Array.copy(dstArrayWithType, 0, newDstArray, 0, counter)
      srcArray = newSrcArray
      dstArrayWithType = newDstArray
      buffer.compact
    }
    this
  }

  /**
   * Projects the buffer into a column. Will rewind after projection.
   * @param columnIdx
   * @param out
   */
  def projectColumnToBuffer(columnIdx: Int, out: ByteBuffer): Unit = {
    EdgeBuffer.projectColumnToBuffer(columnIdx, out, encoderDecoder, buffer.currentBuf, numEdges)
  }


  // Only for internal use..
  def byteArray: Array[Byte] = buffer.currentBuf


  def findOutNeighborsEdges(src: Long, edgeType: Byte) = {
    val n = numEdges
    val results = scala.collection.mutable.ArrayBuffer[DecodedEdge]()
    // Not the most functional way, but should be faster
    var i = 0
    while(i < n) {      // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (srcArray(i) == src && extractType(dstArrayWithType(i)) == edgeType) {
        results += edgeAtPos(i)
      }
      i += 1
    }
    results.toSeq
  }

  def findOutNeighborsCallback(src: Long, callback: QueryCallback, edgeType: Byte) : Unit = {
    val n = numEdges
    val ids = new util.ArrayList[java.lang.Long]()
    val types = new util.ArrayList[java.lang.Byte]()
    val pointers = new util.ArrayList[java.lang.Long]()

    // Not the most functional way, but should be faster
    var i = 0
    while( i < n) {      // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (srcArray(i) == src &&  extractType(dstArrayWithType(i)) == edgeType) {
        ids.add(extractVertexId(dstArrayWithType(i)))
        types.add(extractType(dstArrayWithType(i)))
        pointers.add(PointerUtil.encodeBufferPointer(bufferId, i))
      }
      i += 1
    }
    callback.receiveOutNeighbors(src, ids, types, pointers)
  }

  def find(edgeType: Byte, src: Long, dst: Long) : Option[Long] = {
    var i = 0
    while(i < counter) {
      if (srcArray(i) == src && extractVertexId(dstArrayWithType(i)) == dst && extractType(dstArrayWithType(i)) == edgeType) {
        return Some(PointerUtil.encodeBufferPointer(bufferId, i))
      }
      i += 1
    }
    None
  }



  def findInNeighborsEdges(dst: Long, edgeType: Byte) = {
    val n = numEdges
    val results = scala.collection.mutable.ArrayBuffer[DecodedEdge]()
    // Not the most functional way, but should be faster
    var i = 0
    while(i < n) {   // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (extractVertexId(dstArrayWithType(i)) == dst && extractType(dstArrayWithType(i)) == edgeType) {
        results += edgeAtPos(i)
      }
      i += 1
    }
    results.toSeq
  }

  def findInNeighborsCallback(dst: Long, callback: QueryCallback, edgeType: Byte) : Unit = {
    val n = numEdges
    val ids = new util.ArrayList[java.lang.Long]()
    val types = new util.ArrayList[java.lang.Byte]()
    val pointers = new util.ArrayList[java.lang.Long]()
    // Not the most functional way, but should be faster
    var i = 0
    while(i < n) {   // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (extractVertexId(dstArrayWithType(i)) == dst && extractType(dstArrayWithType(i)) == edgeType) {
        ids.add(srcArray(i))
        types.add(extractType(dstArrayWithType(i)))
        pointers.add(PointerUtil.encodeBufferPointer(bufferId, i))
      }
      i += 1
    }
    callback.receiveInNeighbors(dst, ids, types, pointers)
  }


  def numEdges = counter - deletedEdges

  def edgeIterator = new EdgeIterator {
    var i = (-1)
    def next() : Unit = { i += 1}

    def hasNext = {
      if (i < counter - 1) {
        /* Skip over deleted edges */
        val t = extractType(dstArrayWithType(i + 1))
        if (t != VertexIdTranslate.DELETED_TYPE) {
          true
        }  else {
          i += 1
          hasNext
        }
      } else {
        false
      }
    }

    def getDst = extractVertexId(dstArrayWithType(i))

    def getSrc = srcArray(i)

    def getType = extractType(dstArrayWithType(i))
  }

}

object EdgeBuffer {
  def projectColumnToBuffer(columnIdx: Int, out: ByteBuffer, encoderDecoder: EdgeEncoderDecoder, dataArray: Array[Byte],
                            numEdges: Int): Unit = {

    val edgeSize = encoderDecoder.edgeSize
    val n = numEdges
    val columnLength = encoderDecoder.columnLength(columnIdx)

    if (out.limit() != n * columnLength) {
      throw new IllegalArgumentException("Column buffer not right size: %d != %d".format(out.limit(),
        columnLength * n))
    }
    var i = 0
    val workArray = new Array[Byte](encoderDecoder.columnLength(columnIdx))
    val readBuf = ByteBuffer.wrap(dataArray)
    while(i < n) {
      readBuf.position(i * edgeSize)
      encoderDecoder.readIthColumn(readBuf, columnIdx, out, workArray)
      i += 1
    }
    out.rewind()
  }


}
