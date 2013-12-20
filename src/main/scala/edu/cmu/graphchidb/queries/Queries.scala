package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.queries.frontier.FrontierQueries._

/**
 * Advanced queries
 * @author Aapo Kyrola
 */
object Queries {

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
    val result = friends->traverseOut(edgeType)->traverseOut(edgeType)->selectOut(edgeType, groupByCount)
    result -= friends
    result.results })
  }

      /*
       val initialFrontier = queryVertex(internalId)
      val firstHop =  traverseOut(edgeType)(initialFrontier)
      val secondHop = traverseOut(edgeType)(firstHop)

      val result = groupByCount()
      selectOut(edgeType, result)(secondHop)
      result.results
       */
}
