package edu.cmu.akyrola.prototype

import java.util.Locale
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}
import edu.cmu.graphchidb.compute.Pagerank

/*
// Console
import edu.cmu.akyrola.prototype.TwitterPrototypeDatabase._
     DB.queryOut(DB.originalToInternalId(20))

DB.runIteration(pagerankComputation)

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