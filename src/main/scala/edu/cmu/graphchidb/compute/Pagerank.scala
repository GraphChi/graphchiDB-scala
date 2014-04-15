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
package edu.cmu.graphchidb.compute

import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.storage.Column
import edu.cmu.graphchidb.Util._
import edu.cmu.graphchi.VertexInterval

/**
 * The benchmark everyone does.
 * @author Aapo Kyrola
 */
class Pagerank(db: GraphChiDatabase) extends Computation {

  val columnName = "pagerank"
  var pagerankColumn  = db.column("pagerank", db.vertexIndexing).getOrElse(db.createFloatColumn(columnName, db.vertexIndexing)).asInstanceOf[Column[Float]]

  def computeForInterval(interval: VertexInterval,  minVertex: Long, maxVertex: Long) = {
    val len = (maxVertex - minVertex + 1).toInt
    val accumulators = new Array[Float](len)

    val degreeColumn = db.degreeColumn

    db.sweepInEdgesWithJoin[Float, Long](interval, maxVertex, pagerankColumn, degreeColumn)(
      (src: Long, dst: Long, edgeType: Byte, pagerank: Float, degree: Long) => {
        val outDeg = loBytes(degree)
        if (outDeg > 0) {
          accumulators((dst - minVertex).toInt) += scala.math.max(0.15f, pagerank) / outDeg
        }
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
