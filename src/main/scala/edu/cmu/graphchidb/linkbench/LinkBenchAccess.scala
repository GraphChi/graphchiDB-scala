package edu.cmu.graphchidb.linkbench

import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.queries.Queries
import edu.cmu.graphchidb.queries.internal.{SimpleSetReceiver, SimpleArrayReceiver}
import java.io.{FileWriter, BufferedWriter}
import edu.cmu.graphchi.queries.QueryCallback
import java.{lang, util}

/**
 * For console use
 * @author Aapo Kyrola
 */
object LinkBenchAccess {

  /*
    import edu.cmu.graphchidb.linkbench.LinkBenchAccess._
             DB.shardTree.map( shs => (shs.size, shs.map(_.numEdges).sum) )

   */

  val baseFilename = "/Users/akyrola/graphs/DB/linkbench/linkbench"
  val DB = new GraphChiDatabase(baseFilename, disableDegree = true)

  DB.initialize()

  def inAndOutTest(): Unit = {
     
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
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)
    val foflog = new BufferedWriter(new FileWriter("fof_linkbench.tsv"))

    while(i < 50000) {
      val v = math.abs(r.nextLong() % 9900000) + 1
      val st = System.nanoTime()
      val friendReceiver = new SimpleArrayReceiver(outEdges = true)
      DB.queryOut(DB.originalToInternalId(v), 0, friendReceiver)
      val a = new BitSetOrigIdReceiver(outEdges = true)
      DB.queryOutMultiple(friendReceiver.arr, 0.toByte, a)
      val tFof = System.nanoTime() - st
      val cnt = a.bitset.size
      if (i % 1000 == 0 && cnt >= 0) {
        printf("%d %d fof:%d\n".format(System.currentTimeMillis() - t, i, cnt))
      }
      i += 1
      if (cnt > 0)
          foflog.write("%d,%f\n".format(cnt, tFof * 0.001))

    }

    foflog.close()

  }
}
