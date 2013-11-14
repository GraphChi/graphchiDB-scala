package edu.cmu.akyrola.prototype

import java.util.Locale
import edu.cmu.graphchidb.GraphChiDatabase

/*
// Console
import edu.cmu.akyrola.prototype.TwitterPrototypeDatabase._
queryFollowersAndCountries("kyrpov")

DB.addEdge(23682683, 9999)
DB.addEdge(9998, 23682683)

DB.queryOut(23682683)
val a = DB.queryIn(9999)
a.getRows

queryFollowersAndCountries("biz")


 */

/**
 *
 * @author Aapo Kyrola
 */
object TwitterPrototypeDatabase {

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
  }
}
