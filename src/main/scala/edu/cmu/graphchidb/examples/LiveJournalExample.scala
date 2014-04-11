package edu.cmu.graphchidb.examples

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.util.Random
import edu.cmu.graphchi.GraphChiEnvironment
import scala.io.Source
import java.io.File

import edu.cmu.graphchidb.Util._
import edu.cmu.graphchidb.examples.computation.ConnectedComponentsLabelProp
import edu.cmu.graphchidb.queries.Queries

/**
 * Example application create a database of the live journal graph.
 * We create fake timestamp and weight attributes for each edge (with random values).
 * @author Aapo Kyrola
 */
object LiveJournalExample {

  /**
    * Run in scala console

   import  edu.cmu.graphchidb.examples.LiveJournalExample._

   // To initialize DB
   startIngest

   // Some testing
   recommendFriends(8737)
   recommendFriends(2419)


   *
   */

  val sourceFile =  System.getProperty("user.home")  + "/graphs/soc-LiveJournal1.txt"

  val baseFilename = System.getProperty("user.home")  + "/graphs/DB/livejournal/lj"

  GraphChiDatabaseAdmin.createDatabaseIfNotExists(baseFilename, numShards = 16)

  val DB = new GraphChiDatabase(baseFilename,  numShards = 16)

  /* Create edge columns */
  val timestampColumn = DB.createLongColumn("timestamp", DB.edgeIndexing)
  val weightColumn = DB.createFloatColumn("weight",   DB.edgeIndexing)


  val ccAlgo = new ConnectedComponentsLabelProp(DB)

  DB.initialize()


  def startIngest() {
    async {
      var i = 0
      val r = new Random
      val t = System.currentTimeMillis()
      timed("ingest", {
        val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")

        Source.fromFile(new File(sourceFile)).getLines().foreach( ln => {
          if (!ln.startsWith("#")) {
            val toks = ln.split("\t")
            val from = Integer.parseInt(toks(0))
            val to = Integer.parseInt(toks(1))
            val timestamp = System.currentTimeMillis() - r.nextLong() % 1000000
            val weight = r.nextFloat()

            DB.addEdgeOrigId(0, from, to, timestamp, weight)
            i += 1
            if (i % 1000 == 0) ingestMeter.mark(1000)
            if (i % 1000000 == 0) println((System.currentTimeMillis - t) / 1000 + " s. : Processed: %d".format(i) + " ;" + ingestMeter.getOneMinuteRate + " / sec"
              + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
          }
        })
        DB.flushAllBuffers()
      })
    }
  }


  def connectedComponents() {
      DB.runGraphChiComputation(ccAlgo, 100, enableScheduler=true)
  }

  /**
   * Example: finds the friends-of-friends of user, that are not her friends, and groups them based
   * on how many user's friends are friends of them.
   * Returns top 20 friend of friends that are not my friends */
  def recommendFriends(userIdOrigId: Long) = {
    val userId = DB.originalToInternalId(userIdOrigId)
    val friendsOfFriendsNotMyFriends =  Queries.friendsOfFriendsExcl(userId, 0)(DB)
    friendsOfFriendsNotMyFriends.toSeq.sortBy(-_._2).take(20).map(tup => (DB.internalToOriginalId(tup._1), tup._2))
  }

}
