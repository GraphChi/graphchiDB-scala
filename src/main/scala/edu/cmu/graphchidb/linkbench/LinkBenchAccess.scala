package edu.cmu.graphchidb.linkbench

import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.queries.Queries

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

  def fofTest(): Unit = {
     var i = 1
     val t = System.currentTimeMillis()
     val r = new java.util.Random(260379)

     while(i < 50000) {
        val v = math.abs(r.nextLong() % 100000000) + 1
        val a = Queries.friendsOfFriendsSet(DB.originalToInternalId(v), 0)(DB)
        if (i % 1000 == 0 && a.size >= 0) {
          printf("%d %d fof:%d\n".format(System.currentTimeMillis() - t, i, a.size))
        }
        i += 1
     }

  }
}
