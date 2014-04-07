package edu.cmu.graphchidb.compute

import edu.cmu.graphchi.VertexInterval

/**
 * @author Aapo Kyrola
 */
trait Computation {

  // Should pass maxVertex = actual max vertex in the interval
   def computeForInterval(interval: VertexInterval, minVertex: Long, maxVertex: Long) : Unit
}
