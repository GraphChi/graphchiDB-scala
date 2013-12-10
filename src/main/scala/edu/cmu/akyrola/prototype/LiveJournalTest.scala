package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.io.Source
import java.io.File
import edu.cmu.graphchidb.Util._
import scala.util.Random
import edu.cmu.graphchidb.compute.Pagerank
import edu.cmu.graphchi.GraphChiEnvironment

/**
 * Ingest a full live journal graph from scratch
 * @author Aapo Kyrola
 */
object LiveJournalTest  {

  /**

  import edu.cmu.akyrola.prototype.LiveJournalTest._
  startIngest

    DB.getEdgeValueOrigId(0, 14, timestampColumn)
        DB.updateEdgeOrigId(0, 14, timestampColumn, 333)
    DB.getEdgeValueOrigId(0, 14, timestampColumn)


    DB.queryOut(DB.originalToInternalId(8737)).join(timestampColumn)

   DB.runIteration(pagerankComputation, continuous=true)

   pagerankCol.get(DB.originalToInternalId(8737))

  DB.queryIn(DB.originalToInternalId(8737))

  import edu.cmu.graphchidb.queries.Queries._
    twoHopOut(DB.originalToInternalId(8737))(DB)


    */

  val source =  "/Users/akyrola/graphs/soc-LiveJournal1.txt"

  val baseFilename = "/Users/akyrola/graphs/DB/livejournal/livejournal.txt"

  GraphChiDatabaseAdmin.createDatabase(baseFilename)


  val DB = new GraphChiDatabase(baseFilename)

  /* Create columns */
  val timestampColumn = DB.createIntegerColumn("timestamp", DB.edgeIndexing)
  val typeColumn = DB.createCategoricalColumn("type",  IndexedSeq("follow", "like"), DB.edgeIndexing)

  DB.initialize()

  val pagerankComputation = new Pagerank(DB)
  val pagerankCol = DB.column("pagerank", DB.vertexIndexing).get

  def startIngest() {
    async {
      var i = 0
      val r = new Random
      val t = System.currentTimeMillis()
      timed("ingest", {
        val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")

        Source.fromFile(new File(source)).getLines().foreach( ln => {
          if (!ln.startsWith("#")) {
            val toks = ln.split("\t")
            val from = Integer.parseInt(toks(0))
            val to = Integer.parseInt(toks(1))
            val edgeType = if ((from + to) % 3 == 0) "follow" else "like"
            DB.addEdgeOrigId((i % 2).toByte, from, to, (from + to),
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