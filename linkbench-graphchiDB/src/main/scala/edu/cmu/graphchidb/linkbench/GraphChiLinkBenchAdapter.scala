package edu.cmu.graphchidb.linkbench

import com.facebook.LinkBench._
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import edu.cmu.graphchidb.compute.Pagerank
import edu.cmu.graphchidb.storage.{VarDataColumn, CategoricalColumn, Column}
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import scala.Some

/**
 * @author Aapo Kyrola
 */
class GraphChiLinkBenchAdapter extends GraphStore {


  var currentPhase: Phase = null

  var idSequence = new AtomicLong()

  /**** NODE STORE **/

  def close() = {
    println("GraphChiLinkBenchAdapter: close")
  }

  def clearErrors(threadId: Int) = {
    println("GraphChiLinkBenchAdapter: clear errors")

  }


  val baseFilename = "/Users/akyrola/graphs/DB/linkbench/linkbench"
  var DB : GraphChiDatabase = null


  // TODO!
  def edgeType(typeValue: Long) = (typeValue % 255).toByte

  /* Edge columns */
  var edgeTimestamp : Column[Int] = null
  var edgeVersion: Column[Byte]  = null    // Note, only 8 bits
  // payload data
  // var edgePayLoad: ....

  var nodeTimestamp : Column[Int]  = null
  var nodeVersion: Column[Byte]   = null

  var type0Counters : Column[Int]  = null
  var type1Counters : Column[Int]  = null

  var edgePayloadColumn : VarDataColumn = null
  var vertexPayloadColumn : VarDataColumn = null
  // payload data
  // var nodePaylaod




  def initialize(p1: Properties, phase: Phase, threadId: Int) = {
    println("Initialize: %s, %s, %d".format(p1, currentPhase, threadId))
    currentPhase =  phase
    currentPhase match {
      case Phase.LOAD => {

      }
      case Phase.REQUEST => {

      }
    }

    DB = new GraphChiDatabase(baseFilename)
    /* Create columns */
    edgeTimestamp = DB.createIntegerColumn("time", DB.edgeIndexing)
    edgeVersion = DB.createByteColumn("vers", DB.edgeIndexing)
    edgePayloadColumn = DB.createVarDataColumn("payload", DB.edgeIndexing)

    nodeTimestamp = DB.createIntegerColumn("time", DB.vertexIndexing)
    nodeVersion = DB.createByteColumn("vers", DB.vertexIndexing)
    vertexPayloadColumn = DB.createVarDataColumn("payload", DB.vertexIndexing)

    type0Counters = DB.createIntegerColumn("type0cnt", DB.vertexIndexing)
    type1Counters = DB.createIntegerColumn("type1cnt", DB.vertexIndexing)

    DB.initialize()

  }


  /**
   * Reset node storage to a clean state in shard:
   *   deletes all stored nodes
   *   resets id allocation, with new IDs to be allocated starting from startID
   */
  def resetNodeStore(p1: String, startId: Long) : Unit = {
    println("GraphChiLinkBenchAdapter: reset node store, startId: " + startId)
    GraphChiDatabaseAdmin.createDatabase(baseFilename)
    idSequence.set(startId)
  }



  def addNode(databaseId: String, node: Node) : Long = {
    println("Add node %s, %s".format(databaseId, node))
    /* Just insert. Note: nodetype is ignored. */
    val newId = idSequence.getAndIncrement()
    val newInternalId = DB.originalToInternalId(newId)
    DB.updateVertexRecords(newInternalId)
    nodeTimestamp.set(newInternalId, node.time)
    nodeVersion.set(newInternalId, node.version.toByte)

    // Payload
    val payloadId = vertexPayloadColumn.insert(node.data)
    vertexPayloadColumn.pointerColumn.set(newInternalId, payloadId)

    newId
  }

  def getNode(databaseId: String, nodeType: Int, id: Long) : Node = {
    val internalId = DB.originalToInternalId(id)
    val payloadId = vertexPayloadColumn.pointerColumn.get(internalId).get
    val payloadData = vertexPayloadColumn.get(payloadId)
    val timestamp = nodeTimestamp.get(internalId).getOrElse(0)
    if (timestamp > 0) {
      val versionByte = nodeVersion.get(internalId).getOrElse(0.toByte)
      new Node(id, 0, versionByte, timestamp, payloadData)
    } else {
      null
    }
  }

  def updateNode(databaseId: String, node: Node) = {
    println("Update node %s, %s".format(databaseId, node))
    // TODO: payload
    val internalId = DB.originalToInternalId(node.id)
    val timestamp = nodeTimestamp.get(internalId).getOrElse(0)

    if (timestamp > 0) {
      nodeTimestamp.set(internalId, node.time)
      nodeVersion.set(internalId, node.version.toByte)

      val payloadId = vertexPayloadColumn.pointerColumn.get(internalId).get
      val oldPayload = vertexPayloadColumn.get(payloadId)
      if (!new String(oldPayload).equals(node.data)) {
        // Create new
        val newPayloadId = vertexPayloadColumn.insert(node.data)
        vertexPayloadColumn.pointerColumn.set(internalId, newPayloadId)
        vertexPayloadColumn.delete(payloadId)
      }
      true
    } else {
      false
    }
  }

  def deleteNode(databaseId: String, nodeType: Int, id: Long) = {
    println("Delete node: %s %d".format(nodeType, id))
    val internalId = DB.originalToInternalId(id)
    type0Counters.set(internalId, 0)
    type1Counters.set(internalId, 0)
    DB.deleteVertexOrigId(internalId)
  }

  /**** LINK STORE ****/


  def addLinkImpl(edge: Link) = {
    val edgeTypeByte = edgeType(edge.link_type)
    /* Payload */
    val payloadId = edgePayloadColumn.insert(edge.data)
    DB.addEdgeOrigId(edge.version.toByte, edge.id1, edge.id2, edge.time, edgeTypeByte, payloadId)

    /* Adjust counters */
    // NOTE: hard-coded only two types
    val countColumn = if (edgeTypeByte == 0) type0Counters else type1Counters
    countColumn.update(DB.originalToInternalId(edge.id1), c => c.getOrElse(0) + 1)
  }

  /// NOTE: can apparently ignore noInverse settings!
  def addLink(databaseId: String, edge: Link, noInverse: Boolean) = {
    println("Add link %s, %s".format(edge, noInverse))
    val edgeTypeByte = edgeType(edge.link_type)

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
    println("Delete link: %s %s %s expunge: %s".format(id1, linkType, id2, exPunge))
    if (DB.deleteEdgeOrigId(edgeType(linkType), id1, id2)) {
      val countColumn = if (edgeType(linkType) == 0) type0Counters else type1Counters
      countColumn.update(DB.originalToInternalId(id1), c => c.getOrElse(1) - 1)
      true
    } else {
      false
    }
  }

  def updateLink(databaseId: String, edge: Link, noInverse: Boolean) : Boolean = {
    println("Update link: %s".format(edge))
    val edgeTypeByte = edgeType(edge.link_type)

    DB.findEdgePointer(edgeTypeByte, DB.originalToInternalId(edge.id1), DB.originalToInternalId(edge.id2)) { ptrOpt => {
      ptrOpt match {
        case Some(ptr) => {

          DB.setByPointer(edgeTimestamp, ptr,  edge.time.toInt)
          DB.setByPointer(edgeTimestamp, ptr,   edge.version.toByte)

          val payloadId = DB.getByPointer(edgePayloadColumn.pointerColumn, ptr).get
          val oldPayload = edgePayloadColumn.get(payloadId)
          if (!new String(oldPayload).equals(new String(edge.data))) {
            val newPayloadId = edgePayloadColumn.insert(edge.data)
            DB.setByPointer(edgePayloadColumn.pointerColumn, ptr, newPayloadId)

            edgePayloadColumn.delete(payloadId)
          }

          return true
        }
        case None => throw new IllegalArgumentException("Edge does not exist")
        // TODO: update payload

      }
    } }
    throw new IllegalArgumentException("Edge does not exist: %s".format(edge))
  }

  private def linkFromPointer(ptr: Long, id1: Long, linkType: Long, id2: Long, timestampFilter: Int => Boolean) : Link = {
    val timestamp = DB.getByPointer(edgeTimestamp, ptr)
    if (timestampFilter(timestamp.get)) {
      val version = DB.getByPointer(edgeVersion, ptr)
      val payloadId = DB.getByPointer(edgePayloadColumn.pointerColumn, ptr)
      val payload = edgePayloadColumn.get(payloadId.get)
      // TODO: visibility
      return new Link(id1, linkType, id2, LinkStore.VISIBILITY_DEFAULT, payload, version.get, timestamp.get)
    } else {
      return null
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
    val results = DB.queryOut(DB.originalToInternalId(id1), edgeType(linkType))
    results.getPointers.zip(results.getInternalIds).
      map( row  => linkFromPointer(row._1.asInstanceOf[Long], id1, linkType, DB.internalToOriginalId(row._2.asInstanceOf[Long]), t=>true)
    ).toArray[Link]
  }

  def getLinkList(databaseId: String, id1: Long, linkType: Long, minTimestamp: Long, maxTimestamp: Long,
                  offset: Int, limit: Int) : Array[Link]   = {
    val results = DB.queryOut(DB.originalToInternalId(id1), edgeType(linkType))
    results.getPointers.zip(results.getInternalIds).
      map( row  => linkFromPointer(row._1.asInstanceOf[Long], id1, linkType, DB.internalToOriginalId(row._2.asInstanceOf[Long]),
      t => (t >= minTimestamp && t <= maxTimestamp))
    ).flatten.toArray[Link]
  }

  def countLinks(databaseId: String, id1: Long, linkType: Long) = {
    val internalId = DB.originalToInternalId(id1)
    if (edgeType(linkType) == 1) { type0Counters.get(internalId).getOrElse(0)}
    else {type1Counters.get(internalId).getOrElse(0) }
  }
}
