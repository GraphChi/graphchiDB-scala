package edu.cmu.graphchidb.examples.computation

import edu.cmu.graphchidb.compute.{GraphChiContext, GraphChiVertex, VertexCentricComputation}
import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.storage.Column
import java.util
import java.util.Collections

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
    if (vertex.id == 8053240178L || vertex.id == 1610679185L) {
       vertex.printEdgePtrs
       vertex.edgeValues.foreach(l => println(" bf: %d".format(l)))

    }
    val minLabel = vertex.edgeValues.foldLeft(vertex.id)((mn, label) => math.min(mn, label))
    if (vertex.id == 8053240178L || vertex.id == 1610679185L) {
        println("%d --> %d".format(vertex.id, minLabel))
    }
    if (minLabel != vertex.getData || context.iteration == 0) {
      vertex.setData(minLabel)
      vertex.setAllEdgeValues(minLabel)
      if (vertex.id == 8053240178L || vertex.id == 1610679185L) {
        println("set %d --> %d".format(vertex.id, minLabel))
        vertex.edgeValues.foreach(l => println(" l:%d".format(l)))
      }
      context.scheduler.addTasks(vertex.edgeVertexIds)
    }
  }

  def edgeDataColumn = Some(edgeColumn)
  def vertexDataColumn = Some(vertexColumn)

  // Ugly, straight from java
  def printStats()  = {
    val counts = new util.HashMap[Long, IdCount](1000000)
    vertexColumn.foreach( { (id, label) => {
      if (id != label) {
        var cnt = counts.get(label)
        if (cnt == null) {
          cnt = new IdCount(label, 1)
          counts.put(label, cnt)
        }
        cnt.count += 1
      }
    } })

    val finalCounts = new util.ArrayList(counts.values())
    Collections.sort(finalCounts)
    for(i <- 1 to math.min(20, finalCounts.size())) {
      println(i + ". component: " + finalCounts.get(i - 1))
    }
  }
}
