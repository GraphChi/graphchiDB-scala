package edu.cmu.graphchidb.examples.computation

import edu.cmu.graphchidb.compute.{GraphChiContext, GraphChiVertex, VertexCentricComputation}
import edu.cmu.graphchidb.GraphChiDatabase

/**
 * Label propagation version of connected components. The label of vertex
 * is propagated to all its edges. Note, that Union-Find algorithm is usually much faster --
 * but this is a useful example application.
 * @author Aapo Kyrola
 */
class ConnectedComponentsLabelProp extends VertexCentricComputation[Long, Long] {
  /**
   * Update function to be implemented by an algorithm
   * @param vertex
   * @param context
   * @param database
   */
  def update(vertex: GraphChiVertex[Long, Long], context: GraphChiContext, database: GraphChiDatabase) = {
       val minLabel = if (context.iteration == 0) { vertex.id } else {
         math.min(vertex.id, vertex.edges.foldLeft(0L)((mn, edge) => math.min(mn, edge.getValue))) }
       if (minLabel != vertex.getData) {
          vertex.setData(minLabel)
          vertex.edges.foreach(edge => edge.setValue(minLabel))
          context.scheduler.addTasks(vertex.edges)
       }
  }
}
