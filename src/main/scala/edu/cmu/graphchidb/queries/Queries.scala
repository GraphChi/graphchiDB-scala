package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.GraphChiDatabase

/**
 * Advanced queries
 * @author Aapo Kyrola
 */
object Queries {

  /** Finds friends of friends of search vertex and groups by the number of common
    * followers. Excludes the immediate friends.
    * @param internalId
    * @param edgeType
    * @param DB
    * @return
    */
  def friendsOfFriends(internalId: Long, edgeType: Byte)(implicit DB: GraphChiDatabase) = {
      val firstHop = DB.queryOut(internalId, edgeType)
  }


}
