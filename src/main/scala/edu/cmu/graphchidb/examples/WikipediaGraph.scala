package edu.cmu.graphchidb.examples

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import scala.collection.mutable
import java.io._

import edu.cmu.graphchidb.Util._
import edu.cmu.graphchidb.examples.util.WikipediaParsers
import edu.cmu.graphchi.util.StringToIdMap
import edu.cmu.graphchi.GraphChiEnvironment

/**
 * Example application that reads
 * wikipedia SQL dumps to create a graph. Dumps available from Wikipedia.
 *
 * In the 2014-04-03 dump, there were about 42.4 million pages.
 *
 * @author Aapo Kyrola
 */
object WikipediaGraph {

  /**
  import  edu.cmu.graphchidb.examples.WikipediaGraph._
    */
  val pageInfo =  System.getProperty("user.home")  + "/graphs/wikipedia/enwiki-20140402-page.sql"
  val pageLinks =  System.getProperty("user.home")  + "/graphs/wikipedia/enwiki-20140402-pagelinks.sql"

  val baseFilename = System.getProperty("user.home")  + "/graphs/DB/wikipedia/wikipages"

  val numShards = 128

  val linkType = 0.toByte

  GraphChiDatabaseAdmin.createDatabaseIfNotExists(baseFilename, numShards = numShards)
  val DB = new GraphChiDatabase(baseFilename,  numShards = numShards)


  val pageTitleVarData = DB.createVarDataColumn("pagetitle", DB.vertexIndexing)
  val pageTitlePointer = DB.createLongColumn("pagetitleptr", DB.vertexIndexing)


  DB.initialize()

  def loadPagesFromDump(): Unit = {
    pageTitleVarData.insert("dummy")  // Ensure no-one gets zero-pointer. Ugly
    WikipediaParsers.loadPages(new File(pageInfo), (pageId: Long, namespaceId: Int, pageName: String) =>
    {
      if (namespaceId == 0) {    // Include only main pages for now
      val titlePtr = pageTitleVarData.insert(pageName)
        DB.setVertexColumnValueOrigId(pageId, pageTitlePointer, titlePtr)
        if (pageId % 100000 == 0) println("Process page ID: %d".format(pageId))
      }
    })
    DB.flushAllBuffers()
  }

  def loadLinksFromDump(): Unit = {
    var unsatisfiedLinks = 0L
    var insertedLinks = 0L
    val ingestMeter = GraphChiEnvironment.metrics.meter("edgeingest")
            val t = System.currentTimeMillis()
    WikipediaParsers.loadPageLinks(new File(pageLinks), (fromPageIdOrigId: Long, namespace: Int, toPageName: String) =>
    {
      if (namespace == 0) {
        val toPageOrigId = pageIndex.getId(toPageName)
        if (toPageOrigId >= 0) {
          DB.addEdgeOrigId(linkType, fromPageIdOrigId, toPageOrigId)
          insertedLinks += 1
          if (insertedLinks % 1000 == 0) ingestMeter.mark(1000)
          if (insertedLinks % 1000000 == 0) {
            println("Created %d links, %d could not find destination page (page not created)".format(insertedLinks, unsatisfiedLinks))
            println((System.currentTimeMillis - t) / 1000 + " s. : Processed: %d".format(insertedLinks) + " ;" + ingestMeter.getOneMinuteRate + " / sec"
              + "; mean=" + ingestMeter.getMeanRate + " edges/sec")
          }
        } else {
          unsatisfiedLinks += 1
        }
      }
    })
    println("Created %d links, %d could not find destination page (page not created)".format(insertedLinks, unsatisfiedLinks))
    DB.flushAllBuffers()
  }


  lazy val pageIndex = {
    println("Loading page title index, this may take a while...")
    val idMap = new StringToIdMap(1<<29)

    timed(("load-pageindex"), {
      pageTitlePointer.foreach( (vertexId: Long, namePtr: Long) => {
        try {
          if (namePtr > 0) {
            val title = pageTitleVarData.getString(namePtr)
            idMap.put(title, DB.internalToOriginalId(vertexId).toInt)  // Store the original ID because they are smaller numbers and fit to int
            if (vertexId % 100000 == 0) println("Processing index: %d %d".format(vertexId, namePtr))

          }
        } catch {
          case ia: IllegalArgumentException => println("Could not load title: " + namePtr)
        }

      })
    })
    println("Done loading index")
    idMap
  }

  def pageName(origId: Int) = {
    pageTitleVarData.getString(pageTitlePointer.get(DB.originalToInternalId(origId)).get)
  }

  def populate() = {
     timed("populate", {
      loadPagesFromDump()
      loadLinksFromDump() } )
  }
}
