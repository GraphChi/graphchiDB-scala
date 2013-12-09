package edu.cmu.graphchidb.linkbench

import com.facebook.LinkBench.{Link, Phase, Node, GraphStore}
import java.util.Properties

/**
 * @author Aapo Kyrola
 */
class GraphChiLinkBenchAdapter extends GraphStore {

  /**** NODE STORE **/

  def close() = {
     println("GraphChiLinkBenchAdapter: close")
  }

  def clearErrors(threadId: Int) = {
    println("GraphChiLinkBenchAdapter: clear errors")

  }

  /**
   * Reset node storage to a clean state in shard:
   *   deletes all stored nodes
   *   resets id allocation, with new IDs to be allocated starting from startID
   */
  def resetNodeStore(p1: String, startId: Long) : Unit = {
    println("GraphChiLinkBenchAdapter: reset node store, startId: " + startId)

  }

  def initialize(p1: Properties, currentPhase: Phase, threadId: Int) = {
      println("Initialize: %s, %s, %d".format(p1, currentPhase, threadId))
  }

  def addNode(databaseId: String, node: Node) : Long = {
     println("Add node %s, %s".format(databaseId, node))
     0L
  }

  def getNode(databaseId: String, nodeType: Int, id: Long) : Node = {
     println("Get node: %d, %s".format(nodeType, id))
     null
  }

  def updateNode(databaseId: String, node: Node) = {
    println("Update node %s, %s".format(databaseId, node))
    false
  }

  def deleteNode(databaseId: String, nodeType: Int, id: Long) = {
     println("Delete node: %s %d".format(nodeType, id))
    false
  }

  /**** LINK STORE ****/


  /// NOTE: can apparently ignore noInverse settings!
  def addLink(databaseId: String, edge: Link, noInverse: Boolean) = {
     println("Add link %s, %s".format(edge, noInverse))
     false
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
    false
  }

  def updateLink(databaseId: String, edge: Link, noInverse: Boolean) = {
    println("Update link: %s".format(edge))
    false
  }

  def getLink(databaseId: String, id1: Long, linkType: Long, id2: Long) : Link = {
    println("Get link: %s, %s, %s".format(id1, linkType, id2))
    null
  }

  def getLinkList(databaseId: String, id1: Long, linkType: Long) : Array[Link]    = {
    println("getLinkList %s %s".format(id1, linkType))
    null
  }

  def getLinkList(databaseId: String, id1: Long, linkType: Long, minTimestamp: Long, maxTimestamp: Long,
                  offset: Int, limit: Int) : Array[Link]   = {
    println("getLinkList-range %s %s %s %s offset: %d, limit: %d".format(id1, linkType, minTimestamp, maxTimestamp,
        offset, limit))
    null
  }

  def countLinks(databaseId: String, id1: Long, linkType: Long) = {
    println("countLInks: %s %s".format(id1, linkType))
    0L
  }
}
