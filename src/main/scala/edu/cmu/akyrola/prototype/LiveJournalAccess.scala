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
  */

  val source =  "/Users/akyrola/graphs/soc-LiveJournal1.txt"

  val baseFilename = "/Users/akyrola/graphs/DB/livejournal/livejournal.txt"



  val DB = new GraphChiDatabase(baseFilename, enableVertexShardBits=false, numShards = 64)
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
    val bitset = new util.BitSet()
    def immediateReceive() = true
    def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
      if (outEdges)   bitset.set(DB.internalToOriginalId(dst).toInt)
      else bitset.set(DB.internalToOriginalId(src).toInt)
    }

    def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()
    def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()

  }


  def fofTest(): Unit = {
    var i = 1
    DB.flushAllBuffers()
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)

    while(i <= 15000) {
      val v = math.abs(r.nextLong() % 4500000)
      //   val a = Queries.friendsOfFriendsSet(DB.originalToInternalId(v), 0)(DB)
      val friendReceiver = new SimpleArrayReceiver(outEdges = true)
      DB.queryOut(DB.originalToInternalId(v), 0, friendReceiver)
      val a = new BitSetOrigIdReceiver(outEdges = true)
      DB.queryOutMultiple(friendReceiver.arr, 0.toByte, a)
      val cnt = a.bitset.size
      if (i % 1000 == 0 && cnt >= 0) {
        printf("%d fof %d/%d fof:%d\n".format(System.currentTimeMillis() - t, i, v, cnt))
      }
      i += 1
    }

  }
}
