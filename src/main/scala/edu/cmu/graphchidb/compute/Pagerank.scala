package edu.cmu.graphchidb.compute

import sun.jvm.hotspot.utilities.Interval
import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.storage.Column
import edu.cmu.graphchidb.Util._

/**
 * The benchmark everyone does.
 * @author Aapo Kyrola
 */
class Pagerank(db: GraphChiDatabase) extends Computation {

  val columnName = "pagerank"
  var pagerankColumn  = db.column("pagerank", db.vertexIndexing).getOrElse(db.createFloatColumn(columnName, db.vertexIndexing)).asInstanceOf[Column[Float]]

  def computeForInterval(intervalId: Int, minVertex: Long, maxVertex: Long) = {
      val len = (maxVertex - minVertex + 1).toInt
      val accumulators = new Array[Float](len)

      val degreeColumn = db.degreeColumn

      db.sweepInEdgesWithJoin[Float, Long](intervalId, maxVertex, pagerankColumn, degreeColumn)(
        (src: Long, dst: Long, pagerank: Float, degree: Long) => {
            val outDeg = loBytes(degree)

            if (outDeg <= 0) {
               println("Outdegree <=0 %d vid=%d, %s, authority=%d".format(outDeg, src, degree, db.outDegree(src)))
            }
            assert(outDeg > 0)
            accumulators((dst - minVertex).toInt) += scala.math.max(0.15f, pagerank) / outDeg
        }
      )

      /* Apply */
    val numVertices = db.numVertices
    (minVertex to maxVertex).foreach(vertexId => {
        val newRank = 0.15f / numVertices + (1f - 0.15f) * accumulators((vertexId - minVertex).toInt)
        pagerankColumn.set(vertexId, newRank) // TODO: bulk set
    })
  }
}
