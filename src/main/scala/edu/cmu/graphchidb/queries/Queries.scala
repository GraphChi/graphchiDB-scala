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
package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.queries.frontier.FrontierQueries._
import edu.cmu.graphchidb.storage.Column
import edu.cmu.graphchidb.queries.frontier.{DenseVertexFrontier, VertexFrontier}
import scala.actors.Futures
import scala.collection.mutable.ArrayBuffer
import edu.cmu.graphchi.queries.QueryCallback
import java.{lang, util}

/**
 * Advanced queries
 * @author Aapo Kyrola
 */
object Queries {

  // TODO: join, conditions based on values

  /** Finds friends of friends of search vertex and groups by the number of common
    * followers. Excludes the immediate friends.
    * @param internalId
    * @param edgeType
    * @param db
    * @return
    */
  def friendsOfFriendsExcl(internalId: Long, edgeType: Byte)(implicit db: GraphChiDatabase) = {
    db.timed("FoF", {
      val start = queryVertex(internalId, db)
      val result = start->traverseOut(edgeType)->selectOut(edgeType, groupByCount, dst => !start.hasVertex(dst))
      result.results })
  }

  /** Finds friends of friends of search vertex and groups by the number of common
    * followers. Excludes the immediate friends and limits the first-level friends.
    * @param internalId
    * @param edgeType
    * @param maxFriends
    * @param db
    * @return
    */
  def friendsOfFriendsExclWithLimit(internalId: Long, edgeType: Byte, maxFriends:Int = 100)(implicit db: GraphChiDatabase) = {
    db.timed("FoF-limit", {
      val start = queryVertex(internalId, db)
      val friends = start->traverseOut(edgeType)->limit(maxFriends, randomize=true)
      val result = friends->selectOut(edgeType, groupByCount, dst => !start.hasVertex(dst))
      result.results })
  }


  /**
   * Returns edges from the database that span between the vertices given
   * See http://mathworld.wolfram.com/Vertex-InducedSubgraph.html
   * @param vertices
   * @param db
   * @return
   */

  def inducedSubgraph(vertices: Set[Long], edgeType: Byte)(implicit db: GraphChiDatabase) = {
      /* It suffices to find the out-edges for all the vertices that span between them */
      var subgraphEdges = new ArrayBuffer[ResultEdge]()
      db.queryOutMultiple(vertices.toSeq, edgeType, new QueryCallback {
        def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) = {}
        def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) = {}
        def immediateReceive() = true
        def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
            if (vertices.contains(dst)) {
              subgraphEdges.append(ResultEdge(src, dst, dataPtr))
            }
        }
      })
    subgraphEdges
  }



  def shortestPath(fromInternal: Long, toInternal: Long, maxDepth: Int = 5, edgeType: Byte)(implicit db: GraphChiDatabase) = {
    var frontier = queryVertex(fromInternal, db)
    val visited = new DenseVertexFrontier(db.vertexIndexing, db)
    var passes = 0

    // TODO: this fails on very big graphs. Also hacky to use original IDs here. Reason: original id space assumed
    // Hashtable becomes very slow when the frontier is big. COuld do a switch...
    // to be sequential.
    val parents = new Array[Int](db.numVertices.toInt)
    parents(db.internalToOriginalId(fromInternal).toInt) = db.internalToOriginalId(fromInternal).toInt

    // Search from other direction
    val destInneighbors = scala.actors.Futures.future {
      val inNbrs = db.queryIn(toInternal, edgeType)
      val inBits = new DenseVertexFrontier(db.vertexIndexing, db)
      inNbrs.getInternalIds.foreach( id => inBits.insert(id))
      inBits
    }

    var finished = false  // Ugly
    var foundEarly = false

    while(!frontier.hasVertex(toInternal) && passes < maxDepth && !finished) {
      visited.union(frontier)

      /* Check if any in the frontier is in the in-edges */
      if (destInneighbors.isSet || passes == maxDepth - 1) {
        val inNeighbors = destInneighbors()
        if (inNeighbors.isEmpty) {
          println("No in-neighbors!")
          finished = true
        } else {
          val anyFwdMatch = visited.hasAnyVertex(inNeighbors)
          anyFwdMatch.map { matchId =>
            parents(db.internalToOriginalId(toInternal).toInt) = db.internalToOriginalId(matchId).toInt
            foundEarly = true
            finished = true
          }
          if (passes == maxDepth - 1 && !finished) {
            // Should have been found now
            finished = true
          }
        }
      }

      if (!finished) {
        if (frontier.size > 100000) println("Frontier now: %d iter:%d".format(frontier.size, passes))
        frontier = frontier->traverseOutTopDownDense(edgeType, (src, dst) => {
           if (visited.hasVertex(dst)) { (None, false) }
           else {
             parents(db.internalToOriginalId(dst).toInt) = db.internalToOriginalId(src).toInt

             if (destInneighbors.isSet) {
                if (destInneighbors().hasVertex(dst)) {
                  // Check in-edge list
                  parents(db.internalToOriginalId(toInternal).toInt) = db.internalToOriginalId(dst).toInt
                  foundEarly = true
                  finished  = true
                }
             }

             (Some(dst), dst == toInternal || finished)
           }
        })
        if (!finished) {
          frontier.remove(visited) // Remove visited
        }
        passes += 1
      }
    }

    if (foundEarly || frontier.hasVertex(toInternal)) {
      def parent(dst: Int) : List[Int] = {
        val parentVal = parents(dst)
        val parentVid = parentVal
        if (parentVid == dst) {
          List(dst)
        } else {
          List(dst) ++ parent(parentVid)
        }
      }

      parent(db.internalToOriginalId(toInternal).toInt).map(origId => db.originalToInternalId(origId))

    } else {
      List[Long]()
    }
  }


  // Returns a shortest path tree for unweighted shortest path (basically directed BFS)
  def singleSourceShortestPath(internalId: Long, edgeType: Byte)(implicit db: GraphChiDatabase)  : ShortestPathTree = {
    val ssspColumn = db.createLongColumn("sssp_%s_%s".format(internalId, System.currentTimeMillis()), db.vertexIndexing, temporary = true)
    var frontier = queryVertex(internalId, db)
    ssspColumn.set(internalId, internalId + 1) // ssspColumn has 0 for "infinity" and vertexid + 1 for parent

    while (!frontier.isEmpty) {
      // Add all unvisited out-neighbors to the frontier and set the parent
      frontier =  frontier->traverseOut(edgeType, (src, dst) => {
        if (ssspColumn.getOrElse(dst, 0) == 0) {
          ssspColumn.set(dst, src + 1)
          Some(dst)
        } else None
      })
    }
    new ShortestPathTree(ssspColumn)
  }

  // def inducedSubgraph(vertices: Seq[Long]) : Graph = { }

}

// ssspColumn has 0 for "infinity" and vertexid + 1 for parent
class ShortestPathTree(ssspColumn: Column[Long]) {

  override def finalize() : Unit = {
    println("Deleting shortest path tree column")
    ssspColumn.delete }

  private def parent(dst: Long) : List[Long] = {
    val parentVal = ssspColumn.get(dst).get
    if (parentVal == 0) {
      List[Long]()
    } else {
      val parentVid = parentVal - 1
      if (parentVid == dst) {
        List(dst)
      } else {
        List(dst) ++ parent(parentVid)
      }
    }
  }

  def pathTo(destination: Long) : List[Long] = {
    parent(destination)
  }
}