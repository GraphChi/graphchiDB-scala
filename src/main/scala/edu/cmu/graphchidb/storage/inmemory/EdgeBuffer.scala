package edu.cmu.graphchidb.storage.inmemory

import edu.cmu.graphchidb.{DecodedEdge, EdgeEncoderDecoder}
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import edu.cmu.graphchi.queries.QueryCallback
import java.util

/**
 * Memory-efficient, but unsorted, in-memory edge buffer allowing for fast searches.
 * EdgeBuffer is bound to a specific edge encoding and decoding object.
 * Note: not thread-safe.
 * @author Aapo Kyrola
 */
class EdgeBuffer(encoderDecoder : EdgeEncoderDecoder, initialCapacityNumEdges: Int = 1024) {

  val edgeSize = encoderDecoder.edgeSize
  val tmpBuffer = ByteBuffer.allocate(edgeSize)

  var counter : Int = 0
  var srcArray = new Array[Long](initialCapacityNumEdges)
  var dstArray = new Array[Long](initialCapacityNumEdges)

  /* Create special byteArrayOutStream that allows direct access */
  private val buffer = new ByteArrayOutputStream(initialCapacityNumEdges * encoderDecoder.edgeSize) {
    def currentBuf = buf
  }


  def addEdge(src: Long, dst: Long, values: Any*) = {
    tmpBuffer.rewind()
    encoderDecoder.encode(tmpBuffer, values:_*)
    srcArray(counter) = src
    dstArray(counter) = dst
    counter += 1
    buffer.write(tmpBuffer.array())

    // Expand array. Maybe better to use scala's buffers, but not sure if they have the
    // optimization for primitives.
    if (counter == srcArray.size) {
      var newSrcArray = new Array[Long](counter * 2)
      var newDstArray = new Array[Long](counter * 2)
      Array.copy(srcArray, 0, newSrcArray, 0, srcArray.size)
      Array.copy(dstArray, 0, newDstArray, 0, dstArray.size)
      srcArray = newSrcArray
      dstArray = newDstArray
    }
  }

  private def edgeAtPos(idx: Int) = {
    val buf = ByteBuffer.wrap(buffer.currentBuf, idx * edgeSize, edgeSize)
    encoderDecoder.decode(buf, srcArray(idx), dstArray(idx))
  }

  def findOutNeighborsEdges(src: Long) = {
    val n = numEdges
    val results = scala.collection.mutable.ArrayBuffer[DecodedEdge]()
    // Not the most functional way, but should be faster
    var i = 0
    while( i < n) {      // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (srcArray(i) == src) {
        results += edgeAtPos(i)
      }
      i += 1
    }
    results.toSeq
  }

  def findOutNeighborsCallback(src: Long, callback: QueryCallback) : Unit = {
    val n = numEdges
    val ids = new util.ArrayList[java.lang.Long]()
    val pointers = new util.ArrayList[java.lang.Long]()

    // Not the most functional way, but should be faster
    var i = 0
    while( i < n) {      // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (srcArray(i) == src) {
        ids.add(dstArray(i))
        pointers.add(i)   // TODO: encode edge buffer pointers

      }
      i += 1
    }
    callback.receiveOutNeighbors(src, ids, pointers)
  }



  def findInNeighborsEdges(dst: Long) = {
    val n = numEdges
    val results = scala.collection.mutable.ArrayBuffer[DecodedEdge]()
    // Not the most functional way, but should be faster
    var i = 0
    while(i < n) {   // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (dstArray(i) == dst) {
        results += edgeAtPos(i)
      }
      i += 1
    }
    results.toSeq
  }

  def findInNeighborsCallback(dst: Long, callback: QueryCallback) : Unit = {
    val n = numEdges
    val ids = new util.ArrayList[java.lang.Long]()
    val pointers = new util.ArrayList[java.lang.Long]()
    // Not the most functional way, but should be faster
    var i = 0
    while(i < n) {   // Unfortunately, need to use non-functional while instead of "0 until n" for MUCH better performance
      if (dstArray(i) == dst) {
        ids.add(srcArray(i))
        pointers.add(i)
      }
      i += 1
    }
    callback.receiveInNeighbors(dst, ids, pointers)
  }


  def numEdges = counter

}
