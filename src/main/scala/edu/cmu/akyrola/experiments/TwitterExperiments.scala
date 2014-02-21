
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
import edu.cmu.graphchidb.queries.Queries

/**
 *
 * @author Aapo Kyrola
 */
object TwitterExperiments {
  val baseFilename = "/Users/akyrola/graphs/DB/twitter/twitter_rv.net"
  val DB = new GraphChiDatabase(baseFilename, numShards=256)
  
  //val pagerankComputation = new Pagerank(DB)
  val sdf = new java.text.SimpleDateFormat("YYYYMMDD_HHmmss")

   DB.initialize()


  val pagerankComputation = new Pagerank(DB)
  val pagerankCol = DB.column("pagerank", DB.vertexIndexing).get



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

   def fofTest(n: Int, limit: Int = 200, pagerank: Boolean): Unit = {
    var i = 1
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)
    val id = "%s_%s_i%d_%s".format(InetAddress.getLocalHost.getHostName.substring(0,8), sdf.format(new Date()), n,
      if (pagerank) { "pagerank" } else {""})
     if (pagerank) {
       DB.runIteration(pagerankComputation, continuous=true)
     }
    val foflog = new BufferedWriter(new FileWriter("fof_twitter_%s_limit_%d%s.csv".format(id, limit, if (QueryShard.pinIndexToMemory) { "_pin"} else {""})))

    foflog.write("count,micros\n")

    while(i < n) {
      val v = math.abs(r.nextLong() % 40000000) + 1
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
        //println("%d,%f,%f, %d\n".format(cnt, tFof * 0.001, (st2 - st) * 0.001, v))
        foflog.write("%d,%f\n".format(cnt, tFof * 0.001))
      }
    }

    foflog.close()
    println("Finished")
     System.exit(0)
  }

  def shortestPathTest(n: Int, pagerank: Boolean = false) = {

    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)

    if (pagerank) {
      DB.runIteration(pagerankComputation, continuous=true)
    }

    val id = "%s_%s_i%d_%s".format(InetAddress.getLocalHost.getHostName.substring(0,8), sdf.format(new Date()), n,
      if (pagerank) { "pagerank" } else {""})

    val splog = new FileWriter("shortestpath_twitter_%s_maxdepth_%d.csv".format(id, 5))


    splog.write("pathlength,micros,from,to\n")

    var foundTimes = 0L

    (0 until n).foreach(i => {
      val from = math.abs(r.nextLong() % 40000000) + 1
      val to = math.abs(r.nextLong() % 40000000) + 1
      try {
      val st = System.nanoTime()
      val path = Queries.shortestPath(DB.originalToInternalId(from), DB.originalToInternalId(to), edgeType=0, maxDepth = 5)(DB)
      val tt = System.nanoTime() - st
      println("%d,%d,%d micros:%f".format(from, to, path.size -1, tt*0.001))
      if (path.size > 0) foundTimes += tt
      splog.write("%d,%f,%d,%d\n".format(path.size - 1, tt * 0.001,from,to))
      splog.flush()
      } catch {
          case e: Exception => {
              e.printStackTrace();
              splog.write("err,err,%d,%d\n".format(from, to))
              splog.flush()
          }
      }
    } )

    splog.close()
    println("Total time: " + (System.currentTimeMillis() - t) + " ms n=%d; for found ones =%f".format(n, foundTimes * 0.000001))
  }


  def main(args: Array[String]) {
    if (args(0) == "inout") {
 		   inAndOutTest(args(1).toInt)
    } else if (args(0) == "fof") {
        fofTest(args(1).toInt, pagerank=false)
    } else if (args(0) == "fofpagerank") {
      fofTest(args(1).toInt, pagerank=true)
    } else if (args(0) == "shortestpath") {
       shortestPathTest(args(1).toInt)
    }

  }

 }