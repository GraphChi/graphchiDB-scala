/**
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2014] [Aapo Kyrola / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Publication to cite:  http://arxiv.org/abs/1403.0701
 */

package edu.cmu.graphchidb.examples.computation

import edu.cmu.graphchidb.compute.{GraphChiContext, GraphChiVertex, VertexCentricComputation}
import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.storage.Column
import java.util
import java.util.Collections

/**
 * Label propagation version of connected components. The label of vertex
 * is propagated to all its edges. Note, that Union-Find algorithm is usually much faster --
 * but this is a useful example application nevertheless.
 * @author Aapo Kyrola
 */
class ConnectedComponentsLabelProp(database: GraphChiDatabase)
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
    val minLabel = vertex.edgeValues.foldLeft(vertex.id)((mn, label) => math.min(mn, label))

    if (minLabel != vertex.getData || context.iteration == 0) {
      vertex.setData(minLabel)
      vertex.setAllEdgeValues(minLabel)
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
