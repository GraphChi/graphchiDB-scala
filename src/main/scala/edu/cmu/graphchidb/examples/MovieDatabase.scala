package edu.cmu.graphchidb.examples

import scala.util.Random
import edu.cmu.graphchi.GraphChiEnvironment
import scala.io.Source
import java.io.File

import edu.cmu.graphchidb.Util._
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}
import edu.cmu.graphchidb.examples.computation.ALSMatrixFactorization

/**
 * Example use of GraphChi-DB. Imports Netflix's movie database and computes ALS matrix
 * factorization to produce recommendations.
 * @author Aapo Kyrola
 */
object MovieDatabase {
  /*
  import edu.cmu.graphchidb.examples.MovieDatabase._
  startIngest
  runALS

   */

  // Get data from http://www.select.cs.cmu.edu/code/graphlab/datasets/netflix_mm
  val sourceFile =  System.getProperty("user.home")  +   "/graphs/netflix_mm"
  val movieNamesFile =  System.getProperty("user.home")  +   "/graphs/movie_titles.txt"
  val baseFilename = System.getProperty("user.home")  + "/graphs/DB/moviedb/netflix"


  // User vertex ids are original id + userIdOffset
  // Movie IDs with their original IDs.
  // This is required because GraphChi-DB does not support typed vertices
  val userIdOffset = 200000
  val numShards = 32

  val RATING_EDGE = 0.toByte

  GraphChiDatabaseAdmin.createDatabaseIfNotExists(baseFilename, numShards = numShards)
  val DB = new GraphChiDatabase(baseFilename,  numShards = numShards)

  // Edges for rating
  val ratingEdgeColumn = DB.createByteColumn("rating", DB.edgeIndexing)

  // Movie parameters
  val movieYearColumn = DB.createShortColumn("year", DB.vertexIndexing)

  // Movie name: one column for variable data (the name), and for vertex then a column
  // that contains pointer to the vardata log. This is awkward, but GraphChi-DB is not a full-blown DB...
  val movieNameColumn = DB.createVarDataColumn("moviename", DB.vertexIndexing)
  val movieNamePtrColumn = DB.createLongColumn("movienameptr", DB.vertexIndexing)

  val alsComputation = new ALSMatrixFactorization("alsfactor", ratingEdgeColumn, DB)

  DB.initialize()

  def startIngest = {
    async {
      val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")
      var i = 0
      val t = System.currentTimeMillis()
      Source.fromFile(new File(sourceFile)).getLines().foreach( ln => {
        if (i >= 3 && !ln.startsWith("%")) {    // Skip header
        val toks = ln.split(" ")
          if (toks.length >= 3) {
            val user = userIdOffset + Integer.parseInt(toks(0))
            val movie = Integer.parseInt(toks(1))
            val rating = Integer.parseInt(toks(toks.length - 1))

            DB.addEdgeOrigId(RATING_EDGE, user, movie, rating.toByte)
            if (i % 1000 == 0) ingestMeter.mark(1000)
            if (i % 1000000 == 0) println((System.currentTimeMillis - t) / 1000 + " s. : Processed: %d".format(i) + " ;" + ingestMeter.getOneMinuteRate + " / sec"
              + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
          }

        }
        i += 1
      } )
      DB.flushAllBuffers()

      println("Finished inserting ratings, now populate movie information")

      // Read movie names
      Source.fromFile(new File(movieNamesFile)).getLines().foreach( ln => {
        val toks = ln.split(",")
        val movieId = Integer.parseInt(toks(0))
        val year = if (toks(1) == "NULL") { 0 } else { Integer.parseInt(toks(1))}
        val name = toks(2)

        movieYearColumn.set(movieId, year.toShort)
        val namePtr = movieNameColumn.insert(name)
        movieNamePtrColumn.set(movieId, namePtr)
      })

      println("Done ingest.")

    }

  }

  def runALS = DB.runGraphChiComputation(alsComputation, numIterations=5, enableScheduler=false)


}