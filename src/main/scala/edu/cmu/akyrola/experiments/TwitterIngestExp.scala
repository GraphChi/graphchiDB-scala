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

  DB.initialize()

  def startIngest() {

      var i = 0
      val r = new Random
      val t = System.currentTimeMillis
      val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")

      val sdf = new java.text.SimpleDateFormat("YYYYMMDD_HHmmss")

      val durableBuffers = DB.durableTransactionLog

      val pagerankEnabled = System.getProperty("pagerank", "0") == "1"

      val logFile = new File("twitter_ingest.%s.%s.%s%s".format(
          if (durableBuffers) { "durable" } else {"nondurable"},
          InetAddress.getLocalHost.getHostName.substring(0,8), 
          sdf.format(new Date()), if (pagerankEnabled) { "_pagerank"} else { ""}))


      val logOut = new BufferedWriter(new FileWriter(logFile))
      val pagerankComputation = new Pagerank(DB)

      if (pagerankEnabled) {
          DB.runIteration(pagerankComputation, continuous=true)
      }

      Source.fromInputStream(new FileInputStream(source)).getLines().foreach( ln => {
          if (!ln.startsWith("#")) {

            val toks = ln.split("\t")
            val from = Integer.parseInt(toks(0))
            val to = Integer.parseInt(toks(1))

            DB.addEdgeOrigId(0, from, to)
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