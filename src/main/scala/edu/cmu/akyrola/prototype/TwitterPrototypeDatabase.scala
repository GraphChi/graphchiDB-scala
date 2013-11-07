package edu.cmu.akyrola.prototype

import java.util.Locale
import edu.cmu.graphchidb.GraphChiDatabase

/**
 * @author Aapo Kyrola
 */
object TwitterPrototypeDatabase {

  val baseFilename = "/Users/akyrola/graphs/twitter_rv.net"
  val numShards = 50

  val DB = new GraphChiDatabase(baseFilename, numShards)

  val countries =  Locale.getISOCountries
  val countryColumn = DB.createCategoricalColumn("country", countries, DB.vertexIndexing)


  def queryFollowersAndCountries(userId: Long) = {
    val internalId = DB.originalToInternalId(userId)
    DB.queryIn(internalId).join(countryColumn)
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
