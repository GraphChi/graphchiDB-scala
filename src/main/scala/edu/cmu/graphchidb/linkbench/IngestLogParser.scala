package edu.cmu.graphchidb.linkbench

import scala.io.Source
import java.util.zip.GZIPInputStream
import java.io.{FileWriter, FileInputStream}
import java.text.SimpleDateFormat

/**
 * @author Aapo Kyrola
 */
object IngestLogParser {

  def main(args: Array[String]) {
    val logfile = args(0)
    val out = logfile + "_times.csv"
    val fmt = "yyyy-MM-dd HH:mm:ss,SSS"
    val sdf = new SimpleDateFormat(fmt)
    val outf = new FileWriter(out)
    var startMillis = 0L
    var i = 0
    Source.fromInputStream(new FileInputStream(logfile)).getLines().foreach( ln => {
      val time = ln.substring(5, 5 + fmt.length )
      try {
        i += 1
        val dt = sdf.parse(time)
        if (startMillis == 0) { startMillis = dt.getTime}
        if (ln.indexOf("links/sec avg") > 0 && i % 200 == 0) {
          val suffix = "links loaded at"
          val b = ln.indexOf(suffix)
          val prefix = "id1s/sec avg."
          val a = ln.indexOf(prefix) + prefix.length + 1
          val linkCount =  ln.substring(a, b).trim.toLong

          val s = "%f,%d\n".format((dt.getTime - startMillis) * 0.001 / 3600.0, linkCount)
          outf.write(s)
        }
      } catch {
        case e:Exception => ()
      }
    }

    )
    outf.close()

  }

}
