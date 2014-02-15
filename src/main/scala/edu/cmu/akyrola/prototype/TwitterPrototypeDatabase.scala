package edu.cmu.akyrola.prototype

import java.util.Locale
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}
import edu.cmu.graphchidb.compute.Pagerank
import edu.cmu.graphchi.queries.QueryCallback
import java.{lang, util}
import java.io.{BufferedWriter, FileWriter}
import edu.cmu.graphchidb.queries.internal.SimpleArrayReceiver
import edu.cmu.graphchi.shards.QueryShard

/*
// Console
import edu.cmu.akyrola.prototype.TwitterPrototypeDatabase._
     DB.queryOut(DB.originalToInternalId(20))

DB.runIteration(pagerankComputation)
           import edu.cmu.graphchidb.queries.Queries._
    friendsOfFriends(DB.originalToInternalId(20), 0)(DB)

 */

/**
 *
 * @author Aapo Kyrola
 */
object TwitterPrototypeDatabase {
  val baseFilename = "/Users/akyrola/graphs/DB/twitter/twitter_rv.net"
  val DB = new GraphChiDatabase(baseFilename)
  /* Create columns */
  val timestampColumn = DB.createIntegerColumn("timestamp", DB.edgeIndexing)
  val typeColumn = DB.createCategoricalColumn("type",  IndexedSeq("follow", "like"), DB.edgeIndexing)

  val pagerankComputation = new Pagerank(DB)

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
  def fofTest(n: Int, limit: Int): Unit = {
    var i = 1
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)
    val foflog = new FileWriter("fof_twitter_%d_limit_%d%s.csv".format(n, limit, if (QueryShard.pinIndexToMemory) { "_pin"} else {""}))

    foflog.write("count,micros\n")

    while(i < n) {
      val v = math.abs(r.nextLong() % 40000000) + 1
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

  def fofTest2(n: Int, limit: Int): Unit = {
    var i = 1
    val t = System.currentTimeMillis()
    val r = new java.util.Random(260379)
    val foflog = new FileWriter("fof_twitter_%d_limit_%d_oneonly.csv".format(n, limit))

    foflog.write("count,micros\n")

    while(i < n) {
      val v = 24114418
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

/*
 val baseFilename = "/Users/akyrola/graphs/twitter_rv.net"
  val numShards = 50

  val DB = new GraphChiDatabase(baseFilename, numShards)

  val countries =  Locale.getISOCountries
  val countryColumn = DB.createCategoricalColumn("country", countries, DB.vertexIndexing)
  val nameColumn = DB.createMySQLColumn("twitter_names", "name", DB.vertexIndexing)
  DB.initialize()

  def vertex(name: String) = {
    DB.internalToOriginalId(nameColumn.getByName(name).getOrElse(throw new RuntimeException(name + " not found")))
  }

  def queryFollowersAndCountries(name: String) = {
    val internalId = DB.originalToInternalId(vertex(name))
    DB.queryIn(internalId).join(countryColumn, nameColumn)
  }



  def main(args: Array[String]) {
    println("Total countries: " + countries.size)
    printf("Num vertices %d\n", DB.numVertices)
    /*
     (0 until DB.numVertices.toInt).foreach(v => countryColumn.set(v, (v % countries.size).toByte))
    */
    println("Ready")
  }*/