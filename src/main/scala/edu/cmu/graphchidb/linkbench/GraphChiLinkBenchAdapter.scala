package edu.cmu.graphchidb.linkbench

import com.facebook.LinkBench._
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import edu.cmu.graphchidb.{Util, GraphChiDatabase, GraphChiDatabaseAdmin}
import edu.cmu.graphchidb.storage.{VarDataColumn, Column}
import edu.cmu.graphchidb.storage.ByteConverter
import java.nio.ByteBuffer
import edu.cmu.graphchidb.compute.Pagerank
import edu.cmu.graphchi.GraphChiEnvironment
import edu.cmu.graphchidb.queries.QueryResultWithJoin


class GraphChiLinkBenchAdapter extends GraphStore {
  def close() = GraphChiLinkBenchAdapter.close()

  def clearErrors(p1: Int) = GraphChiLinkBenchAdapter.clearErrors(p1)

  def resetNodeStore(p1: String, p2: Long) = GraphChiLinkBenchAdapter.resetNodeStore(p1, p2)

  def initialize(p1: Properties, p2: Phase, p3: Int) = GraphChiLinkBenchAdapter.initialize(p1, p2, p3)

  def addNode(p1: String, p2: Node) = GraphChiLinkBenchAdapter.addNode(p1, p2)

  def getNode(p1: String, p2: Int, p3: Long) = GraphChiLinkBenchAdapter.getNode(p1, p2, p3)

  def updateNode(p1: String, p2: Node) = GraphChiLinkBenchAdapter.updateNode(p1, p2)

  def deleteNode(p1: String, p2: Int, p3: Long) = GraphChiLinkBenchAdapter.deleteNode(p1, p2, p3)

  def addLink(p1: String, p2: Link, p3: Boolean) = GraphChiLinkBenchAdapter.addLink(p1, p2, p3)

  def deleteLink(p1: String, p2: Long, p3: Long, p4: Long, p5: Boolean, p6: Boolean) = GraphChiLinkBenchAdapter.deleteLink(p1, p2, p3, p4, p5, p6)

  def updateLink(p1: String, p2: Link, p3: Boolean) =
    GraphChiLinkBenchAdapter.updateLink(p1, p2, p3)

  def getLink(p1: String, p2: Long, p3: Long, p4: Long) = GraphChiLinkBenchAdapter.getLink(p1, p2, p3, p4)

  def getLinkList(p1: String, p2: Long, p3: Long) = GraphChiLinkBenchAdapter.getLinkList(p1, p2, p3)

  def getLinkList(p1: String, p2: Long, p3: Long, p4: Long, p5: Long, p6: Int, p7: Int) =
    GraphChiLinkBenchAdapter.getLinkList(p1, p2, p3, p4, p5, p6, p7)

  def countLinks(p1: String, p2: Long, p3: Long) = GraphChiLinkBenchAdapter.countLinks(p1, p2, p3)
}
/**
 * @author Aapo Kyrola
 */
object GraphChiLinkBenchAdapter {


  var currentPhase: Phase = null

  var idSequence = new AtomicLong()

  /**** NODE STORE **/


  val baseFilename = "/Users/akyrola/graphs/DB/linkbench/linkbench"
  var DB : GraphChiDatabase = null


  // TODO!
  def edgeType(typeValue: Long) = (typeValue % 2).toByte

  case class LinkContainer(version: Int, timestamp: Long, payloadId: Long)
  case class NodeContainer(version: Int, timestamp: Int, payloadId: Long)

  /* Edge columns */
  var edgeDataColumn: Column[LinkContainer] = null
  var vertexDataColumn: Column[NodeContainer] = null

  var type0Counters : Column[Int]  = null
  var type1Counters : Column[Int]  = null

  var edgePayloadColumn : VarDataColumn = null
  var vertexPayloadColumn : VarDataColumn = null
  // payload data
  // var nodePaylaod

  var initialized = false

  val workerCounter = new AtomicInteger()

  def close() = {
    println("GraphChiLinkBenchAdapter: close")
    // Last one flushes the buffers
    if (workerCounter.decrementAndGet() == 0) {
      println("Last one -- flushing buffers")
      DB = null
      println("Done.")
      GraphChiEnvironment.reportMetrics()

      initialized = false
    }
  }

  def clearErrors(threadId: Int) = {
    println("GraphChiLinkBenchAdapter: clear errors")
  }


  object LinkToBytesConverter extends ByteConverter[LinkContainer] {
    def fromBytes(bb: ByteBuffer) : LinkContainer = {
      LinkContainer(bb.getInt, bb.getLong, bb.getLong)
    }

    def toBytes(v: LinkContainer, out: ByteBuffer) = {
      out.putInt(v.version)
      out.putLong(v.timestamp)
      out.putLong(v.payloadId)
    }

    def sizeOf = 20
  }

  object NodeToBytesConverter extends ByteConverter[NodeContainer] {
    def fromBytes(bb: ByteBuffer) : NodeContainer = {
      NodeContainer(bb.getInt, bb.getInt, bb.getLong)
    }

    def toBytes(v: NodeContainer, out: ByteBuffer) = {
      out.putInt(v.version)
      out.putInt(v.timestamp)
      out.putLong(v.payloadId)
    }

    def sizeOf = 16
  }

  def initialize(p1: Properties, phase: Phase, threadId: Int) = {
    if (threadId == 0) {
      println("Initialize: %s, %s, %d".format(p1, currentPhase, threadId))
      currentPhase =  phase


      if (currentPhase.ordinal() == Phase.LOAD.ordinal()) {
        println("GraphChiLinkBenchAdapter: reset node store, startId: " + 0)
        GraphChiDatabaseAdmin.createDatabase(baseFilename)
        idSequence.set(0)
      }

      println("Initializing, curphase = " + currentPhase.ordinal())

      DB = new GraphChiDatabase(baseFilename, disableDegree = true)
      /* Create columns */
      edgeDataColumn = DB.createCustomTypeColumn[LinkContainer]("linkdata", DB.edgeIndexing, LinkToBytesConverter)
      vertexDataColumn = DB.createCustomTypeColumn[NodeContainer]("nodedata", DB.vertexIndexing, NodeToBytesConverter)

      edgePayloadColumn = DB.createVarDataColumn("payload", DB.edgeIndexing)
      vertexPayloadColumn = DB.createVarDataColumn("payload", DB.vertexIndexing)

      type0Counters = DB.createIntegerColumn("type0cnt", DB.vertexIndexing)
      type1Counters = DB.createIntegerColumn("type1cnt", DB.vertexIndexing)
      //  val pagerankComputation = new Pagerank(DB)

      DB.initialize()
      initialized = true
      println("Thread " + threadId + " initialized")

      /* Run pagerank */
      //DB.runIteration(pagerankComputation, continuous = true)
    }
    println("Thread " + threadId + " waiting, this=" + this)

    while (!initialized) Thread.sleep(50)
    println("Thread " + threadId + " starting, this=" + this)
    workerCounter.incrementAndGet()
  }


  /**
   * Reset node storage to a clean state in shard:
   *   deletes all stored nodes
   *   resets id allocation, with new IDs to be allocated starting from startID
   */
  def resetNodeStore(p1: String, startId: Long) : Unit = {
    idSequence.set(startId)

  }



  def addNode(databaseId: String, node: Node) : Long = {
    /* Just insert. Note: nodetype is ignored. */
    val newId = idSequence.getAndIncrement
    val newInternalId = DB.originalToInternalId(newId)
    DB.updateVertexRecords(newInternalId)
    val payloadId = vertexPayloadColumn.insert(node.data)
    vertexDataColumn.set(newInternalId, NodeContainer(node.version.toByte, node.time, payloadId))

    newId
  }

  def getNode(databaseId: String, nodeType: Int, id: Long) : Node = {
    val internalId = DB.originalToInternalId(id)

    val vertexData = vertexDataColumn.get(internalId).getOrElse(NodeContainer(0,0,0))
    if (vertexData.timestamp > 0) {
      val payloadData = vertexPayloadColumn.get(vertexData.payloadId)
      new Node(id, 0, vertexData.version, vertexData.timestamp, payloadData)
    } else {
      null
    }
  }

  def updateNode(databaseId: String, node: Node) = {
    // TODO: payload
    val internalId = DB.originalToInternalId(node.id)
    val vertexData = vertexDataColumn.get(internalId).getOrElse(NodeContainer(0,0,0))

    if (vertexData.timestamp > 0) {
      val payloadId = vertexData.payloadId
      val oldPayload = vertexPayloadColumn.get(payloadId)
      val updatedPayloadId = if (!new String(oldPayload).equals(node.data)) {
        // Create new
        val newPayloadId = vertexPayloadColumn.insert(node.data)
        vertexPayloadColumn.delete(payloadId)
        newPayloadId
      } else { payloadId }

      vertexDataColumn.set(internalId, NodeContainer(node.version.toByte, node.time, updatedPayloadId))
      true
    } else {
      false
    }
  }

  def deleteNode(databaseId: String, nodeType: Int, id: Long) = {
    val internalId = DB.originalToInternalId(id)
    /*type0Counters.set(internalId, 0)
    type1Counters.set(internalId, 0)
    DB.deleteVertexOrigId(internalId) */
    // NOTE: as per the mysql link bench, only invalidate the node
    vertexDataColumn.set(internalId, NodeContainer(0,0,0))
    true
  }

  /**** LINK STORE ****/


  def addLinkImpl(edge: Link) = {
    val edgeTypeByte = edgeType(edge.link_type)
    /* Payload */
    val payloadId = edgePayloadColumn.insert(edge.data)
    DB.addEdgeOrigId(edgeTypeByte, edge.id1, edge.id2, LinkContainer(edge.version.toByte, edge.time, payloadId))

    /* Adjust counters */
    // NOTE: hard-coded only two types
    val countColumn = if (edgeTypeByte == 0) type0Counters else type1Counters
    countColumn.update(DB.originalToInternalId(edge.id1), c => {
      val newC = c.getOrElse(0) + 1
      if (newC > 50000 && newC % 10000 == 1) println("High count for %d / %d = %d".format(edge.id1, edgeTypeByte, newC))
      newC
    })
  }

  /// NOTE: can apparently ignore noInverse settings!
  def addLink(databaseId: String, edge: Link, noInverse: Boolean) = {

    if (currentPhase == Phase.LOAD) {
      /* Just insert */
      addLinkImpl(edge)
    } else {
      /* Check first if exits, then insert */
      if (!updateLink(databaseId, edge, noInverse)) {
        addLinkImpl(edge)
      }
    }
    true
  }


  /**
   * Delete link identified by parameters from store
   * @param databaseId
   * @param id1
   * @param linkType
   * @param id2
   * @param noInverse
   * @param exPunge if true, delete permanently.  If false, hide instead
   * @return true if row existed. Implementation is optional, for informational
   *         purposes only.
   * @throws Exception
   */
  def deleteLink(databaseId: String, id1: Long, linkType: Long, id2: Long, noInverse: Boolean, exPunge: Boolean) = {
    if (DB.deleteEdgeOrigId(edgeType(linkType), id1, id2)) {
      val countColumn = if (edgeType(linkType) == 0) type0Counters else type1Counters
      countColumn.update(DB.originalToInternalId(id1), c => c.getOrElse(1) - 1)
      true
    } else {
      false
    }
  }

  def updateLink(databaseId: String, edge: Link, noInverse: Boolean) : Boolean = {
    val edgeTypeByte = edgeType(edge.link_type)

    DB.findEdgePointer(edgeTypeByte, DB.originalToInternalId(edge.id1), DB.originalToInternalId(edge.id2)) { ptrOpt => {
      ptrOpt match {
        case Some(ptr) => {

          val edgeData =  DB.getByPointer(edgeDataColumn, ptr).get

          val payloadId = edgeData.payloadId
          val oldPayload = edgePayloadColumn.get(payloadId)
          val updatedPayloadId = if (!new String(oldPayload).equals(new String(edge.data))) {
            val newPayloadId = edgePayloadColumn.insert(edge.data)

            edgePayloadColumn.delete(payloadId)
            newPayloadId
          } else { payloadId }

          DB.setByPointer(edgeDataColumn, ptr, LinkContainer(edge.version.toByte, edge.time, updatedPayloadId))

          return true
        }
        case None => return false
        // TODO: update payload

      }
    } }
    throw new IllegalArgumentException("Edge does not exist: %s".format(edge))
  }

  private def linkFromPointer(ptr: Long, origid1: Long, linkType: Long, origid2: Long, timestampFilter: Long => Boolean) : Link = {
    val edgeData  = DB.getByPointer(edgeDataColumn, ptr).get
    if (timestampFilter(edgeData.timestamp)) {
      val payload = edgePayloadColumn.get(edgeData.payloadId)
      // TODO: visibility
      new Link(origid1, linkType, origid2, LinkStore.VISIBILITY_DEFAULT, payload, edgeData.version, edgeData.timestamp)
    } else {
      null
    }
  }

  def getLink(databaseId: String, id1: Long, linkType: Long, id2: Long) : Link = {
    DB.findEdgePointer(edgeType(linkType), DB.originalToInternalId(id1), DB.originalToInternalId(id2)) {
      ptrOpt => {
        ptrOpt match {
          case Some(ptr) => {
            return linkFromPointer(ptr, id1, linkType, id2, t => true)
          }
          case None =>
            return null
        }
      }
    }

    null
  }



  def getLinkList(databaseId: String, id1: Long, linkType: Long) : Array[Link]    = {
    val resultReceiver = new QueryResultWithJoin[Link](DB, (src:Long, dst:Long, etype:Byte, ptr:Long) =>
      linkFromPointer(ptr, DB.internalToOriginalId(src), etype, DB.internalToOriginalId(dst),
        t => true))
    DB.queryOut(DB.originalToInternalId(id1), edgeType(linkType), resultReceiver, parallel=true)
    val res = resultReceiver.get.filter(_ != null).toArray[Link]

    if (res.size > 1000) println("res/2 : %d / %d".format(res.size, resultReceiver.get.size))

    if (res.size > 10000) {
       val pivot = Util.QuickSelect.quickSelect(res, 10000, (a: Link, p: Link) => a.time > p.time)
       res.filter(_.time > pivot.time).sortBy(link => -link.time)
    } else {
      res.sortBy(link => -link.time)
    }
  }

  val counter = new AtomicLong()

  def getLinkList(databaseId: String, id1: Long, linkType: Long, minTimestamp: Long, maxTimestamp: Long,
                  offset: Int, limit: Int) : Array[Link]   = {
    val resultReceiver = new QueryResultWithJoin[Link](DB, (src:Long, dst:Long, etype:Byte, ptr:Long) =>
      linkFromPointer(ptr, DB.internalToOriginalId(src), etype, DB.internalToOriginalId(dst),
        t => (t >= minTimestamp && t <= maxTimestamp)))
    DB.queryOut(DB.originalToInternalId(id1), edgeType(linkType), resultReceiver)
    val res = resultReceiver.get.filter(_ != null).toArray[Link]
    println("%d id1: %d res : %d / %d  [ %d --- %d, limit: %d, offset:%d]".format(counter.incrementAndGet(), id1, res.size, resultReceiver.get.size, minTimestamp, maxTimestamp, limit, offset))
    if (res.size > 10000) {
      val pivot = Util.QuickSelect.quickSelect(res, 10000, (a: Link, p: Link) => a.time > p.time)
      val filtered = res.filter(_.time > pivot.time).sortBy(link => -link.time)
      println("filtered size:" + filtered.size)
      filtered
    } else {
      res.sortBy(link => -link.time)
    }
  }

  def countLinks(databaseId: String, id1: Long, linkType: Long) = {
    val internalId = DB.originalToInternalId(id1)
    val c = if (edgeType(linkType) == 0) {
      type0Counters.get(internalId).getOrElse(0)
    } else {
      type1Counters.get(internalId).getOrElse(0)
    }
    c.toLong
  }
}
