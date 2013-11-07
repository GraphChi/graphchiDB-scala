package edu.cmu.graphchidb

import edu.cmu.graphchi.ChiFilenames

/**
 * Defines a sharded graphchi database.
 * @author Aapo Kyrola
 */
class GraphChiDatabase(baseFilename: String, origNumShards: Int) {
   var numShards = origNumShards

   var intervals = ChiFilenames.loadIntervals(baseFilename, origNumShards)


}


