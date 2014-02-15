package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.io.Source
import java.io.{FileWriter, File}
import edu.cmu.graphchidb.Util._
import scala.util.Random
import edu.cmu.graphchidb.compute.Pagerank
import edu.cmu.graphchi.GraphChiEnvironment
import edu.cmu.graphchidb.queries.Queries
import edu.cmu.graphchidb.queries.internal.{SimpleArrayReceiver, SimpleSetReceiver}
import edu.cmu.graphchi.queries.QueryCallback
import java.{lang, util}

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


  class BitSetOrigIdReceiver(outEdges: Boolean) extends QueryCallback {
    val bitset = new util.BitSet(100000000)
    def immediateReceive() = true
    def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
      if (outEdges)   bitset.set(DB.internalToOriginalId(dst).toInt)
      else bitset.set(DB.internalToOriginalId(src).toInt)
    }

    def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()
    def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()

    def size = bitset.cardinality()
  }
  def fofTest(n: Int, limit: Int): Unit = {
    var i = 1
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)
    val foflog = new FileWriter("fof_livejournal_%d_limit_%d.csv".format(n, limit))

    foflog.write("count,micros\n")

    while(i < n) {
      val v = math.abs(r.nextLong() % 4500000) + 1
      val a = new BitSetOrigIdReceiver(outEdges = true)

      val friendReceiver = new SimpleArrayReceiver(outEdges = true, limit=limit)
      val st = System.nanoTime()
      DB.queryOut(DB.originalToInternalId(v), 0, friendReceiver)
      val st2 = System.nanoTime()
      DB.queryOutMultiple(friendReceiver.arr, 0.toByte, a)
      val tFof = System.nanoTime() - st
      val cnt = a.size
      if (i % 1000 == 0 && cnt >= 0) {
        printf("%d %d fof:%d\n".format(System.currentTimeMillis() - t, i, cnt))
      }
      i += 1
      if (cnt > 0)  {
        println("%d,%f,%f, %d\n".format(cnt, tFof * 0.001, (st2 - st) * 0.001, v))
        foflog.write("%d,%f\n".format(cnt, tFof * 0.001))
      }
      foflog.flush()
    }

    foflog.close()
    println("Finished")
  }
  def main(args: Array[String]) {
    println("Initialized...")
  }

}