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
    println( blockName + " " +  (t1 - t0) / 1000000 + "ms")
    result
  }

}
