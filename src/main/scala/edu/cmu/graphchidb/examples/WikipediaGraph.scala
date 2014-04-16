/**
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2014] [Aapo Kyrola / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Publication to cite:  http://arxiv.org/abs/1403.0701
 */

package edu.cmu.graphchidb.examples

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import java.io._

import edu.cmu.graphchidb.Util._
import edu.cmu.graphchidb.examples.util.WikipediaParsers
import edu.cmu.graphchi.util.StringToIdMap
import edu.cmu.graphchi.GraphChiEnvironment

import edu.cmu.graphchidb.queries.Queries
import java.util.concurrent.atomic.AtomicLong

/**
 * Example application that reads
 * wikipedia SQL dumps to create a graph. Dumps available from Wikipedia.
 *
 * In the 2014-04-03 dump, there were about 42.4 million pages and over 300 million edges.
 * Creating the database will take >3 hours on a laptop with SSD.
 *
 * Note, to use this example you need to allocate at least 5 gigs of memory (-Xmx5G)
 *
 * @author Aapo Kyrola
 */
object WikipediaGraph {

  /**

  // Usage in Scala console
  import  edu.cmu.graphchidb.examples.WikipediaGraph._

  // If first time, populate the DB (takes 3 hours on SSD, MacBook Pro)
  populate()

 // Otherwise, and after that, can start playing
 // Note: initialization of the index will take maybe a minute.
   shortestPath("Barack_Obama", "Sauli_Niinisto")
   shortestPath("Helsinki", "Pittsburgh")
   shortestPath("Carnegie_Mellon_University", "Graph")

    // Note that in the beginning the queries are slower as the data is not yet paged into memory.

    */

  val pageInfo =  System.getProperty("user.home")  + "/graphs/wikipedia/enwiki-20140402-page.sql"
  val pageLinks =  System.getProperty("user.home")  + "/graphs/wikipedia/enwiki-20140402-pagelinks.sql"

  val baseFilename = System.getProperty("user.home")  + "/graphs/DB/wikipedia/wikipages"

  val numShards = 128

  val linkType = 0.toByte

  GraphChiDatabaseAdmin.createDatabaseIfNotExists(baseFilename, numShards = numShards)
  implicit val DB = new GraphChiDatabase(baseFilename,  numShards = numShards)


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
    var insertedLinks =  0L
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
    println("Created %d links, could not find destination page for %d links (page not created)".format(insertedLinks, unsatisfiedLinks))
    DB.flushAllBuffers()
    println("FInished insertion.")
  }


  lazy val pageIndex = {
    println("Loading page title index, this may take a while...")
    val names = new StringToIdMap(5000000);

    timed(("load-pageindex"), {
      pageTitlePointer.foreach( (vertexId: Long, namePtr: Long) => {
        try {
          if (namePtr > 0) {
            val title = pageTitleVarData.getString(namePtr)
            names.put(title, DB.internalToOriginalId(vertexId).toInt)
            if (vertexId % 100000 == 0) println("Processing index: %d %d".format(vertexId, namePtr))

          }
        } catch {
          case ia: IllegalArgumentException => println("Could not load title: " + namePtr)
        }

      })

      // Now sort
      names.compute()
      names
    })
  }

  def pageName(origId: Int) = {
    pageTitleVarData.getString(pageTitlePointer.get(DB.originalToInternalId(origId)).get)
  }

  def shortestPath(fromPage: String, toPage: String) = {
    val from = pageIndex.getId(fromPage)
    if (from < 0) throw new IllegalArgumentException("Cannot find page " + fromPage)
    val to = pageIndex.getId(toPage)
    if (to < 0) throw new IllegalArgumentException("Cannot find page " + toPage)
    val path = Queries.shortestPath(DB.originalToInternalId(from), DB.originalToInternalId(to), maxDepth=10, edgeType=0)
    path.map { id => (pageName(DB.internalToOriginalId(id).toInt), DB.internalToOriginalId(id).toInt) }
  }

  def populate() = {
    timed("populate", {
      loadPagesFromDump()
      loadLinksFromDump() } )
  }
}
