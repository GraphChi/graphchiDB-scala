package edu.cmu.graphchidb.compute

/**
 * @author Aapo Kyrola
 */
trait Computation {

  // Should pass maxVertex = actual max vertex in the interval
   def computeForInterval(intervalId: Int, minVertex: Long, maxVertex: Long) : Unit
}
