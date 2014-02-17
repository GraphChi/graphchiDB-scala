package edu.cmu.akyrola.experiments

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.io.Source
import java.io.{FileInputStream, File}
import java.util._
import edu.cmu.graphchidb.Util._
import scala.util.Random
import edu.cmu.graphchi.GraphChiEnvironment
import edu.cmu.graphchidb.compute.Pagerank
import java.net._
import java.io._

/**
 * @author Aapo Kyrola
 */
object TwitterIngestExp {

 
  val sdf = new java.text.SimpleDateFormat("YYYYMMDD_HHmmss")


  val source =  "/Users/akyrola/graphs/twitter_rv.net"
  val baseFilename = "/Users/akyrola/graphs/DB/twitter/twitter_rv.net"

  GraphChiDatabaseAdmin.createDatabase(baseFilename, numShards=64)


  val DB = new GraphChiDatabase(baseFilename, numShards=64)

  /* Create columns */
  val timestampColumn = DB.createIntegerColumn("timestamp", DB.edgeIndexing)
  val typeColumn = DB.createCategoricalColumn("type",  IndexedSeq("follow", "like"), DB.edgeIndexing)
//  val pagerankComputation = new Pagerank(DB)


  DB.initialize()

  def startIngest() {
      var i = 0
      val r = new Random
      val t = System.currentTimeMillis
      val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")

      val sdf = new java.text.SimpleDateFormat("YYYYMMDD_HHmmss")

      val durableBuffers = DB.durableTransactionLog

      val logFile = new File("twitter_ingest.%s.%s.%s".format(
          if (durableBuffers) { "durable" } else {"nondurable"},
          InetAddress.getLocalHost.getHostName.substring(0,8), 
          sdf.format(new Date())))
      val logOut = new BufferedWriter(new FileWriter(logFile))

      Source.fromInputStream(new FileInputStream(source)).getLines().foreach( ln => {
          if (!ln.startsWith("#")) {

            val toks = ln.split("\t")
            val from = Integer.parseInt(toks(0))
            val to = Integer.parseInt(toks(1))
            val edgeType = if ((from + to) % 3 == 0) "follow" else "like"

            DB.addEdgeOrigId(0, from, to, (System.currentTimeMillis() / 1000 - r.nextInt(24 * 3600 * 365 * 5)).toInt,
              typeColumn.indexForName(edgeType))
            i += 1
            if (i % 1000 == 0) ingestMeter.mark(1000)
            if (i % 1000000 == 0) {
                println((System.currentTimeMillis - t) / 1000 + " s. : Processed: %d".format(i) + " ;" + ingestMeter.getOneMinuteRate + " / sec"
                + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
                logOut.write("%f,%d\n".format((System.currentTimeMillis - t) * 0.001, i))
                logOut.flush()
            }   
           
          }
        })
      logOut.close()
  }
  

  def main(args: Array[String]) {
     startIngest()
  }

}