package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.GraphChiDatabase

/**
 * Advanced queries
 * @author Aapo Kyrola
 */
object Queries {

  def twoHopOut(internalId: Long)(implicit DB: GraphChiDatabase) = {
      val firstHop = DB.queryOut(internalId)
      val secondHop = DB.queryOutMultiple(firstHop.getInternalIds.toSet)

      /* Group by */
     secondHop.getVertices.groupBy(identity).mapValues(_.size)
  }


}
