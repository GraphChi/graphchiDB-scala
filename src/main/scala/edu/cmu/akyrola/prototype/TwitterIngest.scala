package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.io.Source
import java.io.File
import edu.cmu.graphchidb.Util._
import scala.util.Random
import edu.cmu.graphchi.GraphChiEnvironment

/**
 * Ingest a full live journal graph from scratch
 * @author Aapo Kyrola
 */
object TwitterIngest {

  /**

  import edu.cmu.akyrola.prototype.TwitterIngest._
  startIngest

    DB.queryOut(DB.originalToInternalId(20))
           DB.queryIn(DB.originalToInternalId(20))

  import edu.cmu.graphchidb.queries.Queries._
    twoHopOut(DB.originalToInternalId(20))(DB)
    */
  val numShards = 256

  val source =  "/Users/akyrola/graphs/twitter_rv.net"

  val baseFilename = "/Users/akyrola/graphs/DB/twitter/twitter_rv.net"

  GraphChiDatabaseAdmin.createDatabase(baseFilename, numShards)


  val DB = new GraphChiDatabase(baseFilename, numShards)

  /* Create columns */
  DB.createIntegerColumn("timestamp", DB.edgeIndexing)
  val typeColumn = DB.createCategoricalColumn("type",  IndexedSeq("follow", "like"), DB.edgeIndexing)

  DB.initialize()

  def startIngest() {
    async {
      var i = 0
      val r = new Random
      val t = System.currentTimeMillis
      val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")


      timed("ingest", {
        Source.fromFile(new File(source)).getLines().foreach( ln => {
          if (!ln.startsWith("#")) {
            val toks = ln.split(" ")
            val from = Integer.parseInt(toks(0))
            val to = Integer.parseInt(toks(1))
            val edgeType = if ((from + to) % 3 == 0) "follow" else "like"
            DB.addEdgeOrigId(from, to, (System.currentTimeMillis() / 1000 - r.nextInt(24 * 3600 * 365 * 5)).toInt,
              typeColumn.indexForName(edgeType))
            i += 1
            if (i % 1000 == 0) ingestMeter.mark(1000)
            if (i % 1000000 == 0) println((System.currentTimeMillis - t) / 1000 + " s. : Processed: %d".format(i) + " ;" + ingestMeter.getOneMinuteRate + " / sec"
                + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
          }
        })
      })
    }
  }

  def main(args: Array[String]) {
    println("Initialized...")
  }

}