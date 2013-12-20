package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.queries.frontier.FrontierQueries._
import edu.cmu.graphchidb.storage.Column

/**
 * Advanced queries
 * @author Aapo Kyrola
 */
object Queries {

  // TODO: join, conditions based on values

  /** Finds friends of friends of search vertex and groups by the number of common
    * followers. Excludes the immediate friends.
    * @param internalId
    * @param edgeType
    * @param db
    * @return
    */
  def friendsOfFriends(internalId: Long, edgeType: Byte)(implicit db: GraphChiDatabase) = {
    db.timed("FoF", {
      val friends = queryVertex(internalId, db)
      val result = friends->traverseOut(edgeType)->traverseOut(edgeType)->selectOut(edgeType, groupByCount, dst => !friends.hasVertex(dst))
      result.results })
  }


  // Returns a shortest path tree for unweighted shortest path (basically directed BFS)
  def singleSourceShortestPath(internalId: Long, edgeType: Byte)(implicit db: GraphChiDatabase)  : ShortestPathTree = {
    val ssspColumn = db.createLongColumn("sssp_%s".format(internalId), db.vertexIndexing, temporary = true)
    var frontier = queryVertex(internalId, db)
    ssspColumn.set(internalId, internalId + 1) // ssspColumn has 0 for "infinity" and vertexid + 1 for parent

    while (!frontier.isEmpty) {
      // Add all unvisited out-neighbors to the frontier and set the parent
      frontier =  frontier->traverseOut(edgeType, (src, dst) => {
        if (ssspColumn.getOrElse(dst, 0) == 0) {
          ssspColumn.set(dst, src + 1)
          Some(dst)
        } else None
      })
    }
    new ShortestPathTree(ssspColumn)
  }

}

// ssspColumn has 0 for "infinity" and vertexid + 1 for parent
class ShortestPathTree(ssspColumn: Column[Long]) {

  override def finalize() : Unit = {
    println("Deleting shortest path tree column")
    ssspColumn.delete }

  private def parent(dst: Long) : List[Long] = {
    val parentVal = ssspColumn.get(dst).get
    if (parentVal == 0) {
      List[Long]()
    } else {
      val parentVid = parentVal - 1
      if (parentVid == dst) {
        List(dst)
      } else {
        List(dst) ++ parent(parentVid)
      }
    }
  }

  def pathTo(destination: Long) : List[Long] = {
    parent(destination)
  }
}