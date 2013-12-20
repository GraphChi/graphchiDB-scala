package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.io.Source
import java.io.{FileInputStream, File}
import edu.cmu.graphchidb.Util._
import scala.util.Random
import edu.cmu.graphchi.GraphChiEnvironment
import edu.cmu.graphchidb.compute.Pagerank
import java.util.zip.GZIPInputStream

/**
 * Ingest a full live journal graph from scratch
 * @author Aapo Kyrola
 */
object TwitterIngest  {

  /**

  import edu.cmu.akyrola.prototype.TwitterIngest._
  startIngest

    DB.findEdgePointer(0, DB.originalToInternalId(20), 2650802847L, debug=true) (a => println(a))


    DB.runIteration(pagerankComputation, continuous=true)

    DB.queryOut(DB.originalToInternalId(20), edgeType=0)
           DB.queryIn(DB.originalToInternalId(20), edgeType=1)

  import edu.cmu.graphchidb.queries.Queries._
    friendsOfFriends(DB.originalToInternalId(20), 0)(DB)

    DB.shardTree.map( shs => (shs.size, shs.map(_.numEdges).sum) )

    */


  val source =  "/Users/akyrola/graphs/twitter_rv.net.gz"
  val baseFilename = "/Users/akyrola/graphs/DB/twitter/twitter_rv.net"

  GraphChiDatabaseAdmin.createDatabase(baseFilename)


  val DB = new GraphChiDatabase(baseFilename)

  /* Create columns */
  val timestampColumn = DB.createIntegerColumn("timestamp", DB.edgeIndexing)
  val typeColumn = DB.createCategoricalColumn("type",  IndexedSeq("follow", "like"), DB.edgeIndexing)
  val pagerankComputation = new Pagerank(DB)
  DB.initialize()

  def startIngest() {
    async {
      var i = 0
      val r = new Random
      val t = System.currentTimeMillis
      val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")

      val checkSet = Set(20, 668, 100000, 13348, 383033, 14583144)
      val inCounters = scala.collection.mutable.HashMap[Int, Int]()
      val outCounters = scala.collection.mutable.HashMap[Int, Int]()

      checkSet.foreach( id => inCounters.put(id, 0))
      checkSet.foreach( id => outCounters.put(id, 0))


      timed("ingest", {
        var foundOnce = false
        Source.fromInputStream(new GZIPInputStream(new FileInputStream(source))).getLines().foreach( ln => {
          if (!ln.startsWith("#")) {
            val toks = ln.split(" ")
            val from = Integer.parseInt(toks(0))
            val to = Integer.parseInt(toks(1))
            val edgeType = if ((from + to) % 3 == 0) "follow" else "like"

            if (checkSet contains from) outCounters(from) = outCounters(from) + 1
            if (checkSet contains to) inCounters(to) = inCounters(to) + 1

            DB.addEdgeOrigId((i % 2).toByte, from, to, (System.currentTimeMillis() / 1000 - r.nextInt(24 * 3600 * 365 * 5)).toInt,
              typeColumn.indexForName(edgeType))
            i += 1
            if (i % 1000 == 0) ingestMeter.mark(1000)
            if (i % 1000000 == 0) println((System.currentTimeMillis - t) / 1000 + " s. : Processed: %d".format(i) + " ;" + ingestMeter.getOneMinuteRate + " / sec"
                + "; mean=" + ingestMeter.getMeanRate + " edges/sec")

            /* Consistency check */
            if (i % 333333 == 0) {
               // Degree check
              checkSet.foreach(id =>
              {
                val indeg =  DB.inDegree(DB.originalToInternalId(id))
                val outdeg = DB.outDegree(DB.originalToInternalId(id))
                val expected = inCounters(id) + outCounters(id)
                if (!(indeg + outdeg == expected)) {
                  printf("MISMATCH! %d / %d, %d / %d\n".format(indeg, inCounters(id), outdeg, outCounters(id)));
                  throw new IllegalStateException()
                }
              })
            }

            /* Search constistency check */
            if (i % 5000000 == 0) {
                checkSet.foreach(id =>
                {
                  val ins0 =  DB.queryIn(DB.originalToInternalId(id), edgeType=0).getInternalIds
                  val ins1 =  DB.queryIn(DB.originalToInternalId(id), edgeType=1).getInternalIds
                  val ins = ins0 ++ ins1

                  val outs0 =  DB.queryOut(DB.originalToInternalId(id), edgeType=0).getInternalIds
                  val outs1 =  DB.queryOut(DB.originalToInternalId(id), edgeType=1).getInternalIds
                  val outs = outs0 ++ outs1
                  val expected = inCounters(id) + outCounters(id)

                   printf("%d: ins=%d:%d / %d outs=%d:%d / %d sum=%d expected=%d\n".format(id, ins0.size, ins1.size, inCounters(id),
                     outs0.size, outs.size, outCounters(id),
                      ins.size + outs.size, expected))

                   if (!(ins.size + outs.size == expected)) {
                      printf("MISMATCH!\n")
                   }

                   if (id == 20) {
                      var containsX = outs0.contains(2650802847L)
                      println("Contains 2650802847L: %s".format(containsX))
                      if (containsX) {
                          foundOnce = true
                         DB.findEdgePointer(0, DB.originalToInternalId(20), 2650802847L, debug=true)
                         { a => { println(a)

                          if (!a.isDefined) println("2650802847L  in 20's out list but not found! -- abort")  }
                         }
                      } else if (foundOnce) {
                        println("2650802847L not in 20's out list anymore! -- abort")
                        return
                      }
                   }
                }
                )
            }
          }
        })
      })
    }
  }

  def main(args: Array[String]) {
     startIngest()
  }

}