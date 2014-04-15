package edu.cmu.graphchidb.examples

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.util.Random
import edu.cmu.graphchi.GraphChiEnvironment
import scala.io.Source
import java.io.File

import edu.cmu.graphchidb.Util._
import edu.cmu.graphchidb.examples.computation.ConnectedComponentsLabelProp
import edu.cmu.graphchidb.queries.Queries
import edu.cmu.graphchidb.compute.Pagerank

/**
 * Example application for social network graph.
 * We create fake timestamp and weight attributes for each edge (with random values). This is just for demonstration.
 * @author Aapo Kyrola
 */
object SocialNetworkExample {

  /**
    * Run in scala console

   import  edu.cmu.graphchidb.examples.SocialNetworkExample._

   // To initialize DB (you need to do this only on your first session)
   startIngest

   // Some testing
   recommendFriends(8737)
   recommendFriends(2419)
   recommendFriendsLimited(2419)

   DB.queryIn(DB.originalToInternalId(2409), 0)
   DB.queryOut(DB.originalToInternalId(8737), 0)

   // To run connected components
   connectedComponents()

   // After a while, you can ask
   ccAlgo.printStats

   // To get a vertex component label (which might not be yet the final one)
  ccComponentColumn.get(DB.originalToInternalId(8737)).get

   // To get pagerank of a vertex (note, that it is being continuously updated), so this
   // just looks up the value.
   pagerankCol.get(DB.originalToInternalId(8737)).get

   *
   */


  val sourceFile =  System.getProperty("user.home")  + "/graphs/soc-LiveJournal1.txt"
  val baseFilename = System.getProperty("user.home")  + "/graphs/DB/livejournal/lj"

  GraphChiDatabaseAdmin.createDatabaseIfNotExists(baseFilename, numShards = 16)

  implicit val DB = new GraphChiDatabase(baseFilename,  numShards = 16)

  /* Create edge columns */
  val timestampColumn = DB.createLongColumn("timestamp", DB.edgeIndexing)
  val weightColumn = DB.createFloatColumn("weight",   DB.edgeIndexing)

  /* Pagerank -- run in background continuously */
  val pagerankComputation = new Pagerank(DB)
  val pagerankCol = DB.column("pagerank", DB.vertexIndexing).get

  /* Connected components (run connectedComponents()) */
  val ccAlgo = new ConnectedComponentsLabelProp(DB)
  val ccComponentColumn = ccAlgo.vertexDataColumn.get

  DB.initialize()

  /* Start pagerank */
  DB.runIteration(pagerankComputation, continuous=true)


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
    async {
      println("Running connected components in background...")
      DB.runGraphChiComputation(ccAlgo, 100, enableScheduler=true)
    }
  }

  /**
   * Example: finds the friends-of-friends of user, that are not her friends, and groups them based
   * on how many user's friends are friends of them. Note: this can be very slow when user has many friends.
   * See function recommendFriendsLimited() for a more efficient, but approximate version.
   * Returns top 20 friend of friends that are not my friends */
  def recommendFriends(userIdOrigId: Long) = {
    val userId = DB.originalToInternalId(userIdOrigId)
    val friendsOfFriendsNotMyFriends =  Queries.friendsOfFriendsExcl(userId, 0)
    friendsOfFriendsNotMyFriends.toSeq.sortBy(-_._2).take(20).map(tup => (DB.internalToOriginalId(tup._1), tup._2))
  }


  def recommendFriendsLimited(userIdOrigId: Long) = {
    val userId = DB.originalToInternalId(userIdOrigId)
    val friendsOfFriendsNotMyFriends =  Queries.friendsOfFriendsExclWithLimit(userId, 0, maxFriends = 50)
    friendsOfFriendsNotMyFriends.toSeq.sortBy(-_._2).take(20).map(tup => (DB.internalToOriginalId(tup._1), tup._2))
  }

  /**
   * Find shortest path between two users
   */
  def shortestPath(userFromOrigId: Long, userToOrigId: Long) = {
     val path = Queries.shortestPath(DB.originalToInternalId(userFromOrigId), DB.originalToInternalId(userToOrigId), maxDepth=5, edgeType=0)
     path.map { id => DB.internalToOriginalId(id) }
  }


  /* Example of join */
  def queryOutWithTimestamps(vertexOrigId: Long) = {
      DB.queryOut(DB.originalToInternalId(vertexOrigId), 0).join(timestampColumn)
  }

}
