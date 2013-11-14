package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.io.Source
import java.io.File
import edu.cmu.graphchidb.Util._
import scala.util.Random

/**
 * Ingest a full live journal graph from scratch
 * @author Aapo Kyrola
 */
object LiveJournalTest {

  /**

  import edu.cmu.akyrola.prototype.LiveJournalTest._
  startIngest

    DB.queryOut(DB.originalToInternalId(8737))
   */
  val numShards = 64

  val source =  "/Users/akyrola/graphs/soc-LiveJournal1.txt"

  val baseFilename = "/Users/akyrola/graphs/DB/livejournal/livejournal.txt"

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
      timed("ingest", {
        Source.fromFile(new File(source)).getLines().foreach( ln => {
          if (!ln.startsWith("#")) {
            val toks = ln.split("\t")
            val from = Integer.parseInt(toks(0))
            val to = Integer.parseInt(toks(1))
            val edgeType = if ((from + to) % 3 == 0) "follow" else "like"
            DB.addEdgeOrigId(from, to, (System.currentTimeMillis() / 1000 - r.nextInt(24 * 3600 * 365 * 5)).toInt,
              typeColumn.indexForName(edgeType))
            i += 1
            if (i % 1000000 == 0) println("Processed: %d".format(i))
          }
        })
      })
    }
  }

  def main(args: Array[String]) {
    println("Initialized...")
  }

}