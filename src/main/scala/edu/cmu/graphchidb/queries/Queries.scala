package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.GraphChiDatabase

/**
 * Advanced queries
 * @author Aapo Kyrola
 */
object Queries {

  def twoHopOut(internalId: Long)(implicit DB: GraphChiDatabase) = {
      val firstHop = DB.queryOut(internalId)
      val secondHop = DB.queryOutMultiple(firstHop.getRows.toSet)

      /* Group by */
     secondHop.getRows.groupBy(identity).mapValues(_.size)
  }


}
