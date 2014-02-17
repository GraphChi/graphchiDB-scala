
// Experiments on the twitter databse

package edu.cmu.akyrola.experiments

import java.util.Locale
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}
import edu.cmu.graphchidb.compute.Pagerank
import edu.cmu.graphchi.queries.QueryCallback
import java.{lang, util}
import java.io.{BufferedWriter, FileWriter}
import edu.cmu.graphchidb.queries.internal.SimpleArrayReceiver
import edu.cmu.graphchi.shards.QueryShard

import java.util._
import java.text._
import java.net._
import edu.cmu.graphchi.GraphChiEnvironment
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * @author Aapo Kyrola
 */
object TwitterExperiments {
  val baseFilename = "/Users/akyrola/graphs/DB/twitter/twitter_rv.net"
  val DB = new GraphChiDatabase(baseFilename, numShards=64)
  
  //val pagerankComputation = new Pagerank(DB)
  val sdf = new java.text.SimpleDateFormat("YYYYMMDD_HHmmss")

  DB.initialize()

  println(DB.columns)

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

  class DummyReceiver() extends QueryCallback {
     val counter = new AtomicInteger

    def immediateReceive() = true
    def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
       counter.incrementAndGet()
    }

    def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()
    def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()

    def size = counter.get()
  }


  def inAndOutTest(iterations: Int) {
 val r = new java.util.Random(260379)
    var i = 1

    val id = "%s_%s_i%d".format(InetAddress.getLocalHost.getHostName.substring(0,8), sdf.format(new Date()), iterations)

    val qlog = new BufferedWriter(new FileWriter("inout_twitter_%s.pin_%s.tsv".format(id, QueryShard.pinIndexToMemory)))
    qlog.write("outsize,outtime,insize,intime\n")


    val queryMeter = GraphChiEnvironment.metrics.meter("queries")

    (0 to iterations).foreach ( i => {
      val v = DB.originalToInternalId(math.abs(r.nextLong() % 65000000))
      val inRecv = new DummyReceiver()

      val tInSt = System.nanoTime()
       DB.queryIn(v, 0, inRecv)
      val tIn = System.nanoTime() - tInSt

      val outRecv = new DummyReceiver()

      val tOutSt = System.nanoTime()  
      DB.queryOut(v, 0, outRecv, parallel = true)
      val tOut = System.nanoTime() - tOutSt
      if (i % 10 == 0) queryMeter.mark(10)

      this.synchronized {
	      qlog.write("%d,%f,".format(outRecv.size, tOut * 0.001))
    	  qlog.write("%d,%f\n".format(inRecv.size, tIn * 0.001))
    	}
      	if (i%1000 == 0) println("%d/%d, 1 minute rate %f".format(i, iterations, queryMeter.getOneMinuteRate))
     })
    qlog.close()
  }

   def fofTest(n: Int, limit: Int = 200): Unit = {
    var i = 1
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)
    val id = "%s_%s_i%d".format(InetAddress.getLocalHost.getHostName.substring(0,8), sdf.format(new Date()), n)

    val foflog = new BufferedWriter(new FileWriter("fof_twitter_%s_limit_%d%s.csv".format(id, limit, if (QueryShard.pinIndexToMemory) { "_pin"} else {""})))

    foflog.write("count,micros\n")

    while(i < n) {
      val v = math.abs(r.nextLong() % 4000000) + 1
      val a = new BitSetOrigIdReceiver(outEdges = true)

      val friendReceiver = new SimpleArrayReceiver(outEdges = true, limit=limit)
      val st = System.nanoTime()
      DB.queryOut(DB.originalToInternalId(v), 0, friendReceiver, parallel=true)
      val st2 = System.nanoTime()
      DB.queryOutMultiple(friendReceiver.arr, 0.toByte, a, parallel=true)
      val tFof = System.nanoTime() - st
      val cnt = a.size
      if (i % 1000 == 0 && cnt >= 0) {
        printf("%d %d fof:%d\n".format(System.currentTimeMillis() - t, i, cnt))
        foflog.flush()
      }
      i += 1
      if (cnt > 0)  {
        println("%d,%f,%f, %d\n".format(cnt, tFof * 0.001, (st2 - st) * 0.001, v))
        foflog.write("%d,%f\n".format(cnt, tFof * 0.001))
      }
    }

    foflog.close()
    println("Finished")
  }



  def main(args: Array[String]) {
    if (args(0) == "inout") {
 		   inAndOutTest(args(1).toInt)
    } else {
        fofTest(args(1).toInt)
    }
  }

 }