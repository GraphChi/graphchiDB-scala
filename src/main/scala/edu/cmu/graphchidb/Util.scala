package edu.cmu.graphchidb

import java.nio.ByteBuffer

/**
 * Random utility functions
 * @author Aapo Kyrola
 */
object Util {


  def timed[R](blockName: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println( blockName + " " +  (t1 - t0) / 1000000.0 + "ms")
    result
  }

  // http://www.jroller.com/vaclav/entry/asynchronous_methods_in_scala
  def async(fn: => Unit): Unit = scala.actors.Actor.actor { fn }

  def loBytes(x: Long) : Int = (x & 0xffffffff).toInt
  def hiBytes(x: Long) : Int = ((x >> 32) & 0xffffffff).toInt
  def setLo(x: Int, y:Long) : Long = (y & 0xffffffff00000000L) | x
  def setHi(x: Int, y:Long) : Long = (y & 0x00000000ffffffffL) | (x.toLong << 32)
  def setHiLo(hi: Int, lo: Int) : Long = setHi(hi, setLo(lo, 0L))
  def setBit(x: Long, idx:Int) = x | (1L << idx)
  def getBit(x: Long, idx:Int): Boolean = (x & (1L << idx)) != 0

  def intToByteArray(x: Int) = {
     ByteBuffer.allocate(4).putInt(x).array()
  }

  def intFromByteArray(arr : Array[Byte]) : Int = {
     ByteBuffer.wrap(arr).getInt
  }

}
