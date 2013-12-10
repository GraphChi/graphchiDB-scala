package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.GraphChiDatabase

/**
 * Advanced queries
 * @author Aapo Kyrola
 */
object Queries {

  def twoHopOut(internalId: Long, edgeType: Byte)(implicit DB: GraphChiDatabase) = {
      val firstHop = DB.queryOut(internalId, edgeType)
      val secondHop = DB.queryOutMultiple(firstHop.getInternalIds.toSet, edgeType)

      /* Group by */
     secondHop.getVertices.groupBy(identity).mapValues(_.size)
  }


}
