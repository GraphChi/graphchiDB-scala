package edu.cmu.graphchidb.linkbench

import edu.cmu.graphchidb.GraphChiDatabase

/**
 * For console use
 * @author Aapo Kyrola
 */
object LinkBenchAccess {

  /*
    import edu.cmu.graphchidb.linkbench.LinkBenchAccess._
    DB.initialize()
            DB.shardTree.map( shs => (shs.size, shs.map(_.numEdges).sum) )

   */

  val baseFilename = "/Users/akyrola/graphs/DB/linkbench/linkbench"
  val DB = new GraphChiDatabase(baseFilename, disableDegree = true)
}
