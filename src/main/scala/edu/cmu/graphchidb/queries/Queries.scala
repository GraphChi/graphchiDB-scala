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
      val initialFrontier = queryVertex(internalId)
      val firstHop =  traverseOut(edgeType)(db, initialFrontier)
      val secondHop = traverseOut(edgeType)
  }


}
