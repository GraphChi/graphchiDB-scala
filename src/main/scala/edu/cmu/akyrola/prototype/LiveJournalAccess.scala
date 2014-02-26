package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.queries.internal.{SimpleSetReceiver, SimpleArrayReceiver}
import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import java.io.{FileWriter, BufferedWriter}
import edu.cmu.graphchi.queries.QueryCallback
import scala.collection.mutable
import java.{lang, util}

/**
 * @author Aapo Kyrola
 */
object LiveJournalAccess {
            /*
  import edu.cmu.akyrola.prototype.LiveJournalAccess._
  import edu.cmu.graphchidb.queries.Queries
  Queries.shortestPath(DB.originalToInternalId(8737), 3489673880L, edgeType=0)(DB)
    Queries.shortestPath(DB.originalToInternalId(8737), DB.originalToInternalId(2409), edgeType=0)(DB)

  */

  val source =  "/Users/akyrola/graphs/soc-LiveJournal1.txt"

  val baseFilename = "/Users/akyrola/graphs/DB/livejournal/livejournal.txt"



  val DB = new GraphChiDatabase(baseFilename, enableVertexShardBits=false, numShards = 16)
  DB.initialize()

  def inAndOutTest(): Unit = {
    DB.flushAllBuffers()
    val r = new java.util.Random(260379)
    var i = 1

    val outlog = new BufferedWriter(new FileWriter("out_livejournal.tsv"))
    val inlog = new BufferedWriter(new FileWriter("in_livejournal.tsv"))

    while(i <= 50000) {
      val v = DB.originalToInternalId(math.abs(r.nextLong() % 4500000))
      val tInSt = System.nanoTime()
      val inRecv = new SimpleSetReceiver(outEdges = false)
       DB.queryIn(v, 0, inRecv)
      val tIn = System.nanoTime() - tInSt

      val outRecv = new SimpleSetReceiver(outEdges = true)
      DB.queryOut(v, 0, outRecv)
      val tOut = System.nanoTime() - tInSt

      outlog.write("%d,%f\n".format(outRecv.set.size, tOut * 0.001))
      inlog.write("%d,%f\n".format(inRecv.set.size, tIn * 0.001))
      i += 1
      if (i%1000 == 0) println("%d/%d".format(i, 50000))
    }
     inlog.close()
    outlog.close()
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
}
