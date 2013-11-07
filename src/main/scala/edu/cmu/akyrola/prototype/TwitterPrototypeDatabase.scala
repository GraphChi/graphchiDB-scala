package edu.cmu.akyrola.prototype

import edu.cmu.graphchidb.storage.GraphChiDatabase
import java.util.Locale

/**
 * @author Aapo Kyrola
 */
object TwitterPrototypeDatabase {

  def countries() : Seq[String] = {
    Locale.getISOCountries
  }

  def main(args: Array[String]) {
      val baseFilename = "/Users/akyrola/graphs/twitter_rv.net"
      val numShards = 50

      val DB = new GraphChiDatabase(baseFilename, numShards)

     println(countries())
     println("Total countries: " + countries().size)
     // val countryColumn = new CategoricalColumn("country", countries())
  }
}
