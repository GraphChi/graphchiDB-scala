package edu.cmu.graphchidb

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

}
