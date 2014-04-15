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

package edu.cmu.graphchidb.compute

import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.storage.Column
import scala.actors.threadpool.AtomicInteger

/**
 * Classes for Vertex Centric computation in GraphChi-DB
 * Code bit unscalaish internally to optimize the memory usage
 * @author Aapo Kyrola
 */
trait VertexCentricComputation[VertexValueType, EdgeValueType] {

  /**
   * Update function to be implemented by an algorithm
   * @param vertex
   * @param context
   */
  def update(vertex: GraphChiVertex[VertexValueType, EdgeValueType], context: GraphChiContext)

  /* Callbacks, similar to what GraphChi does */
  def beforeIteration(context: GraphChiContext) = {}
  def afterIteration(context: GraphChiContext) = {}

  def edgeDataColumn : Option[Column[EdgeValueType]]
  def vertexDataColumn : Option[Column[VertexValueType]]

  def isParallel = false
}


class GraphChiContext(val iteration: Int, val maxIterations: Int, val scheduler: Scheduler) {
}



class GraphChiEdge[EdgeDataType](val vertexId: Long, dataPtr: Long, dataColumn: Option[Column[EdgeDataType]], database: GraphChiDatabase) {

  def getValue : EdgeDataType = dataColumn match {
    case Some(column: Column[EdgeDataType]) => database.getByPointer(column, dataPtr).get
    case None => throw new RuntimeException("Tried to get edge value but no edge data column defined")
  }

  def setValue(newVal: EdgeDataType) =  dataColumn match {
    case Some(column:Column[EdgeDataType]) => database.setByPointer(column, dataPtr, newVal)
    case None => throw new RuntimeException("Tried to set edge value but no edge data column defined")
  }

}

class GraphChiVertex[VertexDataType, EdgeDataType](val id: Long, database: GraphChiDatabase,
                                                   vertexDataColumn: Option[Column[VertexDataType]],
                                                   edgeDataColumn: Option[Column[EdgeDataType]],
                                                   val inDegree: Int, val outDegree: Int) {
  /* Internal specification of edges */
  var inc = 0
  var outc = 0
  // First in-edges, then out-edges. Alternating tuples (vertex-id, dataPtr)
  private val edgeSpec = new Array[Long]((inDegree + outDegree) * 2)


  def addInEdge(vertexId: Long, dataPtr: Long) : Unit = {
    this.synchronized {
      if (inc < inDegree) {
        val i = inc
        inc += 1

        edgeSpec(i * 2) = vertexId
        edgeSpec(i * 2 + 1) = dataPtr
      } // If more edges were added during the construction of this vertex, ignore them. Questionnable!
    }
  }

  def addOutEdge(vertexId: Long, dataPtr: Long) : Unit = {
    this.synchronized {
      if (outc < outDegree) {
        val i = outc  + inDegree
        outc += 1

        edgeSpec(i * 2) = vertexId
        edgeSpec(i * 2 + 1) = dataPtr
      }
    }
  }

  private def adjust(j: Int) =  if (inc < inDegree && j >= inc) { j + (inDegree - inc)} else { j } // Adjust for if got less edges than expected


  def edge(j : Int) : GraphChiEdge[EdgeDataType] = {
    val i = adjust(j)
    new GraphChiEdge[EdgeDataType](edgeSpec(i * 2), edgeSpec(i * 2 + 1), edgeDataColumn, database)
  }

  def inEdge(i: Int) = if (i < inDegree) { edge(i) } else
  { throw new ArrayIndexOutOfBoundsException("Asked in-edge %d, but in-degree only %d".format(i, inDegree))}

  def outEdge(i : Int)  =  if (i < outDegree) { edge(i + inDegree) } else
  { throw new ArrayIndexOutOfBoundsException("Asked in-edge %d, but in-degree only %d".format(i, outDegree))}

  def edges = (0 until getNumEdges).iterator.map {i => edge(i)}

  // Debug
  def printEdgePtrs() =  (0 until getNumEdges).foreach {i => println("%d:%d".format(edgeSpec(adjust(i) * 2), edgeSpec(adjust(i) * 2 + 1))) }

  def edgeValues =  (0 until getNumEdges).iterator.map {i =>  database.getByPointer(edgeDataColumn.get, edgeSpec(adjust(i) * 2 + 1)).get}
  def edgeVertexIds =  (0 until getNumEdges).iterator.map {i =>  edgeSpec(adjust(i) * 2)}

  def setAllEdgeValues(newVal: EdgeDataType) =
    (0 until getNumEdges).foreach {i => database.setByPointer(edgeDataColumn.get, edgeSpec(adjust(i) * 2 + 1), newVal)}

  def setAllOutEdgeValues(newVal: EdgeDataType) =
    (inDegree until getNumEdges).foreach {i => database.setByPointer(edgeDataColumn.get, edgeSpec(adjust(i) * 2 + 1), newVal)}



  def inEdges = (0 until getNumOutEdges).iterator.map {i => outEdge(i)}
  def outEdges = (0 until getNumInEdges).iterator.map {i => inEdge(i)}

  def getNumInEdges = inc
  def getNumOutEdges = outc
  def getNumEdges = inc + outc

  def getData = vertexDataColumn match {
    case Some(column: Column[VertexDataType])=> column.get(id).get
    case None => throw new RuntimeException("Tried to get vertex data, btu vertex data column not defined")
  }
  def setData(newVal: VertexDataType) = vertexDataColumn match {
    case Some(column: Column[VertexDataType]) => column.set(id, newVal)
    case None => throw new RuntimeException("Tried to set vertex data, btu vertex data column not defined")
  }
}
