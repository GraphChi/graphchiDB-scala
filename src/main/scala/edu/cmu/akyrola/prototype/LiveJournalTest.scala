package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.io.Source
import java.io.File
import edu.cmu.graphchidb.Util._
import scala.util.Random
import edu.cmu.graphchidb.compute.Pagerank
import edu.cmu.graphchi.GraphChiEnvironment
import edu.cmu.graphchidb.queries.Queries
import edu.cmu.graphchidb.queries.internal.{SimpleArrayReceiver, SimpleSetReceiver}

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


    DB.queryOut(DB.originalToInternalId(8737), 0).join(timestampColumn)

   DB.runIteration(pagerankComputation, continuous=true)

   pagerankCol.get(DB.originalToInternalId(8737))

  DB.queryIn(DB.originalToInternalId(8737))

  import edu.cmu.graphchidb.queries.Queries._
  val sp = singleSourceShortestPath(DB.originalToInternalId(8737), 0)(DB)
  sp.pathTo(DB.originalToInternalId(2419))
  val sp2 = singleSourceShortestPath(DB.originalToInternalId(8737), 1)(DB)
  sp2.pathTo(DB.originalToInternalId(2419))

    friendsOfFriends(DB.originalToInternalId(8737))(DB)
             friendsOfFriends(DB.originalToInternalId(1000))(DB)


    */

  val source =  "/Users/akyrola/graphs/soc-LiveJournal1.txt"

  val baseFilename = "/Users/akyrola/graphs/DB/livejournal/livejournal.txt"

  GraphChiDatabaseAdmin.createDatabase(baseFilename, numShards = 64)


  val DB = new GraphChiDatabase(baseFilename, enableVertexShardBits=false, numShards = 64)

  /* Create columns */
  /* val timestampColumn = DB.createIntegerColumn("timestamp", DB.edgeIndexing)
   val typeColumn = DB.createCategoricalColumn("type",  IndexedSeq("follow", "like"), DB.edgeIndexing)
    */
  DB.initialize()
  /*
val pagerankComputation = new Pagerank(DB)
val pagerankCol = DB.column("pagerank", DB.vertexIndexing).get
   */

  def testIn(origId: Long) = {
    val x = DB.originalToInternalId(origId)
    val ins = DB.queryIn(x, 0).getInternalIds
    ins.foreach { a =>
      val outs = DB.queryOut(a, 0).getInternalIds
      if (!outs.contains(x)) {
        throw new IllegalStateException()
      }
    }
    true
  }

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
            DB.addEdgeOrigId(0, from, to) /*, (from + to),
              typeColumn.indexForName(edgeType)) */
            i += 1
            if (i % 1000 == 0) ingestMeter.mark(1000)
            if (i % 1000000 == 0) println((System.currentTimeMillis - t) / 1000 + " s. : Processed: %d".format(i) + " ;" + ingestMeter.getOneMinuteRate + " / sec"
              + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
          }
        })
      })
    }
  }

  def fofTest(): Unit = {
    var i = 1
    DB.flushAllBuffers()
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)

    while(i <= 50000) {
      val v = math.abs(r.nextLong() % 4500000) + 1
      //   val a = Queries.friendsOfFriendsSet(DB.originalToInternalId(v), 0)(DB)
      val friendReceiver = new SimpleArrayReceiver(outEdges = true)
      DB.queryOut(DB.originalToInternalId(v), 0, friendReceiver)
      val a = new SimpleSetReceiver(outEdges = true)
      DB.queryOutMultiple(friendReceiver.arr, 0.toByte, a)
      val cnt = a.set.size
      if (i % 1000 == 0 && cnt >= 0) {
        printf("%d %d fof:%d\n".format(System.currentTimeMillis() - t, i, cnt))
      }
      i += 1
    }

  }

  def main(args: Array[String]) {
    println("Initialized...")
  }

}