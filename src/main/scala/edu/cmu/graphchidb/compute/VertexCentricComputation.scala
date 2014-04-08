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
}


class GraphChiContext(val iteration: Int, val maxIterations: Int, val scheduler: Scheduler) {
}



class GraphChiEdge[EdgeDataType](val vertexId: Long, dataPtr: Long, dataColumn: Option[Column[EdgeDataType]], database: GraphChiDatabase) {

  def getValue : EdgeDataType = dataColumn match {
    case column: Column[EdgeDataType] => database.getByPointer(column, dataPtr).get
    case None => throw new RuntimeException("Tried to get edge value but no edge data column defined")
  }

  def setValue(newVal: EdgeDataType) =  dataColumn match {
    case column: Column[EdgeDataType] => database.setByPointer(column, dataPtr, newVal)
    case None => throw new RuntimeException("Tried to set edge value but no edge data column defined")
  }

}

class GraphChiVertex[VertexDataType, EdgeDataType](val id: Long, database: GraphChiDatabase,
                                                   vertexDataColumn: Option[Column[VertexDataType]],
                                                   edgeDataColumn: Option[Column[EdgeDataType]],
                                                   val inDegree: Int, val outDegree: Int) {
  /* Internal specification of edges */
  val inc = new AtomicInteger(0)
  val outc = new AtomicInteger(0)
  // First in-edges, then out-edges. Alternating tuples (vertex-id, dataPtr)
  private val edgeSpec = new Array[Long]((inDegree + outDegree) * 2)


  def addInEdge(vertexId: Long, dataPtr: Long) : Unit = {
    val i = inc.getAndIncrement
    if (inc.get() > inDegree) {
      System.err.println("Mismatch vertex " + id + " inc=" + inc + " inDeg=" + inDegree)
    }
    edgeSpec(i * 2) = vertexId
    edgeSpec(i * 2 + 1) = dataPtr
  }

  def addOutEdge(vertexId: Long, dataPtr: Long) : Unit = {
    val i = outc.getAndIncrement  + inDegree
    if (outc.get() > outDegree) {
      System.err.println("Mismatch vertex " + id + " outc=" + outc + " outDeg=" + outDegree)
      assert(false)
    }
    edgeSpec(i * 2) = vertexId
    edgeSpec(i * 2 + 1) = dataPtr
  }

  def edge(i : Int) : GraphChiEdge[EdgeDataType] =
    new GraphChiEdge[EdgeDataType](edgeSpec(i * 2), edgeSpec(i * 2 + 1), edgeDataColumn, database)

  def inEdge(i: Int) = if (i < inDegree) { edge(i) } else
  { throw new ArrayIndexOutOfBoundsException("Asked in-edge %d, but in-degree only %d".format(i, inDegree))}

  def outEdge(i : Int)  =  if (i < outDegree) { edge(i + inDegree) } else
  { throw new ArrayIndexOutOfBoundsException("Asked in-edge %d, but in-degree only %d".format(i, outDegree))}

  def edges = (0 until getNumEdges).toStream.map {i => edge(i)}
  def inEdges = (0 until getNumOutEdges).toStream.map {i => outEdge(i)}
  def outEdges = (0 until getNumInEdges).toStream.map {i => inEdge(i)}

  def getNumInEdges = inDegree
  def getNumOutEdges = outDegree
  def getNumEdges = inDegree + outDegree

  def getData = vertexDataColumn match {
    case column : Column[VertexDataType] => column.get(id).get
    case None => throw new RuntimeException("Tried to get vertex data, btu vertex data column not defined")
  }
  def setData(newVal: VertexDataType) = vertexDataColumn match {
    case column : Column[VertexDataType] => column.set(id, newVal)
    case None => throw new RuntimeException("Tried to set vertex data, btu vertex data column not defined")
  }
}
