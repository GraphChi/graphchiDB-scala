package edu.cmu.graphchidb.examples.computation

import edu.cmu.graphchidb.compute.{GraphChiContext, GraphChiVertex, VertexCentricComputation}
import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.storage.Column

/**
 * Label propagation version of connected components. The label of vertex
 * is propagated to all its edges. Note, that Union-Find algorithm is usually much faster --
 * but this is a useful example application.
 * @author Aapo Kyrola
 */
class ConnectedComponentsLabelProp( database: GraphChiDatabase)
    extends VertexCentricComputation[Long, Long] {

   private val vertexColumn = database.createLongColumn("cc", database.vertexIndexing)
   private val edgeColumn = database.createLongColumn("cce", database.edgeIndexing)
   edgeColumn.autoFillEdgeFunc =  Some((src: Long, dst: Long, edgeType: Byte) => math.min(src, dst))
   vertexColumn.autoFillVertexFunc = Some((id: Long) => id)


  /**
   * Update function to be implemented by an algorithm
   * @param vertex
   * @param context
   */
  def update(vertex: GraphChiVertex[Long, Long], context: GraphChiContext) = {
    // debug
    if (vertex.inc.get != vertex.inDegree) {
      System.err.println("Mismatch in indeg: " + vertex.inc.get + " / " + vertex.inDegree)
    }
   // assert(vertex.inc.get == vertex.inDegree)
    if (vertex.inc.get != vertex.inDegree) {
      System.err.println("Mismatch in outdeg: " + vertex.outc.get + " / " + vertex.outDegree)
    }
    assert(vertex.outc.get == vertex.outDegree)

    val minLabel = if (context.iteration == 0) { vertex.id } else {
         math.min(vertex.id, vertex.edges.foldLeft(0L)((mn, edge) => math.min(mn, edge.getValue))) }
       if (minLabel != vertex.getData) {
          vertex.setData(minLabel)
          vertex.edges.foreach(edge => edge.setValue(minLabel))
          context.scheduler.addTasks(vertex.edges)
       }
  }

  def edgeDataColumn = Some(edgeColumn)
  def vertexDataColumn = Some(vertexColumn)
}
