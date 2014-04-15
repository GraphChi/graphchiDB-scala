package edu.cmu.graphchidb.examples.util

import edu.cmu.graphchidb.Util._
import java.io.{File, FileInputStream, InputStreamReader, BufferedReader}
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author Aapo Kyrola
 */
object WikipediaParsers {

  /**
   * Loads wikipedia page dump
   * @param pageInfoFile
   * @param insertFn  function that is called for each page (pageid, namespace-id, page-title)
   */
  def loadPages(pageInfoFile: File, insertFn: (Long, Int, String) => Unit) : Unit = {
    timed("load-wikipedia-pages", {
      // Very hacky
      val stream =  new BufferedReader(new InputStreamReader(new FileInputStream(pageInfoFile)))
      // Find the beginning
      while (!stream.readLine().contains("ALTER TABLE `page`")) {}

      // Now we are where the data starts
      var chunk = 32678
      val chars = new Array[Char](chunk)

      var finished = false
      var leftover = ""
      while (!finished) {
        var a =  stream.read(chars, 0, chunk)
        var chunkStr = leftover + new String(chars)



        def processChunk(str: String) = {
          // Very ugly, should use some state machine or proper parser
          var st = 0

          var finished = false
          try {
            while(!finished) {
              val stnext = str.indexOf("(", st)
              if (stnext >= 0) {
                st = stnext
                val nx = str.indexOf(",", st+1)
                if (nx > st) {
                  val pageId = str.substring(st + 1, nx).toLong

                  val b = str.indexOf("'", st)
                  if (b > 0) {
                    val namespace = str.substring(nx +1, str.indexOf(",", nx+1)).toInt

                    val c = str.indexOf("','", b+1)
                    if (c > 0) {
                      val pageName = str.substring(b + 1, c)
                      val next = str.indexOf(")", c)
                      if (next > 0) {
                        insertFn(pageId, namespace, pageName)
                        st = next
                      } else finished = true
                    } else finished = true
                  } else finished = true
                } else finished = true
              } else  finished = true
            }
          } catch {
            case e:Exception => {
              println("st=%d".format(st))
              println(str)
              throw e
            }
          }
          str.substring(st)
        }

        leftover = processChunk(chunkStr)

        if (a < chunk) finished = true
      }
    } )
  }


  def loadPageLinks(linkFile: File, insertFn: (Long, Int, String) => Unit) = {
    timed("load-wikipedia-links", {
      // Very hacky
      val stream =  new BufferedReader(new InputStreamReader(new FileInputStream(linkFile)))
      // Find the beginning
      while (!stream.readLine().contains("ALTER TABLE `pagelinks`")) {}

      // Now we are where the data starts
      var chunk = 65536
      val chars = new Array[Char](chunk)

      var finished = false
      var leftover = ""
      var parCount = new AtomicInteger()
      while (!finished) {
        var a =  stream.read(chars, 0, chunk)
        var chunkStr = leftover + new String(chars)



        def processLinkChunk(str: String) = {
          // Very ugly, should use some state machine or proper parser
          var st = 0
          val pages = new ArrayBuffer[Tuple3[Long, Int, String]]()
          var finished = false
          try {
            while(!finished) {
              val stnext = str.indexOf("(", st)
              if (stnext >= 0) {
                st = stnext
                val nx = str.indexOf(",", st+1)
                if (nx > st) {
                  val pageId = str.substring(st + 1, nx).toLong
                  val b = str.indexOf("'", st)
                  if (b > 0) {
                    val namespace = str.substring(nx +1, str.indexOf(",", nx+1)).toInt

                    val c =  str.indexOf("'),(", b+1)
                    if (c > 0) {
                      val pageName = str.substring(b + 1, c)
                      val next = str.indexOf(")", c)
                      if (next > 0) {
                        pages.append((pageId, namespace, pageName))
                        st = next
                      } else finished = true
                    } else finished = true
                  } else finished = true
                } else finished = true
              } else  finished = true
            }
          } catch {
            case e:Exception => {
              println("st=%d".format(st))
              println(str)
              throw e
            }
          }
          // Handle insertions async -- but do not allow too many parallel calls
          while(parCount.get() > 8) { Thread.sleep(100) }

          async {
             parCount.incrementAndGet()
             pages.foreach(pg => insertFn(pg._1, pg._2, pg._3))
             parCount.getAndDecrement
          }

          str.substring(st)
        }

        leftover = processLinkChunk(chunkStr)

        if (a < chunk) finished = true
      }

    })
  }
}
