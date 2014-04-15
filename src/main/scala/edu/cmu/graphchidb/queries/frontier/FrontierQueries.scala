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
package edu.cmu.graphchidb.queries.frontier

import edu.cmu.graphchidb.GraphChiDatabase
import scala.collection.mutable
import edu.cmu.graphchi.queries.{FinishQueryException, QueryCallback}
import java.{lang, util}

/**
 * @author Aapo Kyrola
 */
object FrontierQueries {

  // TODO: queryShard.queryOut with direct one-edge callback

  type queryFunction = (VertexFrontier) => VertexFrontier
  type denseQueryFunction = (VertexFrontier) => DenseVertexFrontier


  def queryVertex[T](vid: Long,  db:GraphChiDatabase) : VertexFrontier = {
    new SparseVertexFrontier(db.vertexIndexing, Set(vid), db)
  }

  def step[R](sparseBlock: (SparseVertexFrontier) => R,
              denseBlock: (DenseVertexFrontier) => R):  (VertexFrontier) => R = {
    (frontier: VertexFrontier) => {
      val frontierSize = frontier.size
      lazy val denseFrontier = frontier match {
        case dense: DenseVertexFrontier => dense
        case sparse: SparseVertexFrontier => sparse.toDense
      }
      lazy val sparseFrontier =  frontier match {
        case dense: DenseVertexFrontier => dense.toSparse
        case sparse: SparseVertexFrontier => sparse
      }

      // TODO: proper cost analysis
      if (frontierSize < 100000) {
        sparseBlock(sparseFrontier)
      } else {
        denseBlock(denseFrontier)
      }
    }
  }


  class FrontierCallback(db: GraphChiDatabase) extends QueryCallback {
    val frontier = new DenseVertexFrontier(db.vertexIndexing, db)
    def immediateReceive() = true

    def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = frontier.insert(dst)

    def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()
    def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()
  }


  def limit(maxSize: Int, randomize: Boolean) : queryFunction = (frontier : VertexFrontier) => frontier.limit(maxSize, randomize)

  /* Creates new frontier of the out-neighbors of the frontier */
  def traverseOut(edgeType: Byte) : queryFunction = {
    def topDown(frontier:SparseVertexFrontier) : VertexFrontier = {
      val frontierCallback = new FrontierCallback(frontier.db)
      frontier.db.queryOutMultiple(frontier.toSeq, edgeType, frontierCallback)
      frontierCallback.frontier
    }

    def bottomUp(frontier:DenseVertexFrontier) : VertexFrontier  = {
      val outFrontier = new DenseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      frontier.db.sweepAllEdges() (
        (src: Long, dst: Long, eType: Byte ) => {
          if (edgeType == eType && frontier.hasVertex(src)) outFrontier.insert(dst)
        }
      )
      outFrontier
    }
    step(topDown, bottomUp)
  }




  // Function returns item to add to frontier and a boolean that determines whether to continue
  def traverseOut(edgeType: Byte, fn: (Long, Long) => Option[Long]) : queryFunction = {
    def topDown(frontier:SparseVertexFrontier) : VertexFrontier = {
      val frontierSet = frontier.toSet

      val newFrontier = if (frontierSet.size > 20000) {   // TODO: remove hard coding
        new DenseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      } else {
        new SparseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      }

      frontier.db.queryOutMultiple(frontierSet.toSeq, edgeType, new QueryCallback {
        def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) {}
        def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]){}

        def immediateReceive() = true
        def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) {
          val v = fn(src, dst)
          if (v.isDefined) newFrontier.insert(v.get)
        }
      })

      newFrontier
    }

    def bottomUp(frontier:DenseVertexFrontier) : VertexFrontier  = {
      val outFrontier = new DenseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      frontier.db.sweepAllEdges() (
        (src: Long, dst: Long, eType: Byte ) => {
          if (edgeType == eType && frontier.hasVertex(src))  {
            val v = fn(src, dst)
            if (v.isDefined) outFrontier.insert(v.get)
          }
        }
      )
      outFrontier
    }
    step(topDown, bottomUp)
  }



  def traverseOutTopDownDense(edgeType: Byte, fn: (Long, Long) => Tuple2[Option[Long], Boolean]) : denseQueryFunction = {
    def topDown(frontier:VertexFrontier) : DenseVertexFrontier = {

      val newFrontier =  new DenseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      var finished = false

      if (frontier.size < frontier.db.numVertices / 8) {

      frontier.db.queryOutMultiple(frontier.toSeq, edgeType, new QueryCallback {
        def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) {}
        def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]){}

        def immediateReceive() = true
        def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) {
          val (v, doFinish) = fn(src, dst)
          if (v.isDefined) newFrontier.insert(v.get)
          if (doFinish || finished) {
            finished = true
            throw new FinishQueryException()
          }
        }
      }, parallel=true)
      } else {
         //
        println("Use bottom-up as frontier size so big: %d".format(frontier.size))
        frontier.db.sweepAllEdges() (
          (src: Long, dst: Long, eType: Byte ) => {
            if (edgeType == eType && frontier.hasVertex(src))  {
              val (v, doFinish) = fn(src, dst)
              if (v.isDefined) newFrontier.insert(v.get)
              if (doFinish || finished) {
                finished = true
                throw new FinishQueryException()
              }
            }
          }
        )
      }

      newFrontier
    }

    topDown
  }

  def traverseOutUntil(edgeType: Byte, fn: (Long, Long) => Tuple2[Option[Long], Boolean]) : queryFunction = {
    def topDown(frontier:SparseVertexFrontier) : VertexFrontier = {
      val frontierSet = frontier.toSet

      val newFrontier = if (frontierSet.size > 20000) {   // TODO: remove hard coding
        new DenseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      } else {
        new SparseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      }

      var finished = false

      frontier.db.queryOutMultiple(frontierSet.toSeq, edgeType,new QueryCallback {
        def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) {}
        def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]){}

        def immediateReceive() = true
        def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) {
          val (v, doFinish) = fn(src, dst)
          if (v.isDefined) newFrontier.insert(v.get)
          if (doFinish || finished) {
            finished = true
            throw new FinishQueryException()
          }
        }
      }, parallel=true)

      newFrontier
    }

    def bottomUp(frontier:DenseVertexFrontier) : VertexFrontier  = {
      val outFrontier = new DenseVertexFrontier(frontier.db.vertexIndexing, frontier.db)
      var finished = false

      try {
        frontier.db.sweepAllEdges() (
          (src: Long, dst: Long, eType: Byte ) => {
            if (edgeType == eType && frontier.hasVertex(src))  {
              val (v, doFinish) = fn(src, dst)
              if (v.isDefined) outFrontier.insert(v.get)
              if (doFinish || finished) {
                finished = true
                throw new FinishQueryException()
              }
            }
          }
        )
        outFrontier
      } catch {
        case fqe : FinishQueryException => outFrontier
      }
    }
    step(topDown, bottomUp)
  }


  trait VertexResultReceiver {
    def apply(vid: Long) : Unit
  }


  // TODO: optimization for bottomUp: set min and max src for sweepAll

  /* Emits each out-neighbor for the frontier */
  def selectOut[RV <: VertexResultReceiver](edgeType: Byte, resultReceiver: RV, condition: ( Long) => Boolean = (u) => true) = {
    def topDown(frontier: SparseVertexFrontier) : RV = {
      frontier.db.queryOutMultiple(frontier.toSet.toSeq, edgeType, new QueryCallback {
        def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) = {}
        def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) = { }

        def immediateReceive() = true

        def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) {
          if (condition(dst)) resultReceiver(dst)
        }
      })
      resultReceiver
    }
    def bottomUp(frontier: DenseVertexFrontier) : RV = {
      frontier.db.sweepAllEdges() {
        (src: Long, dst: Long, eType: Byte ) => {
          if (edgeType == eType && frontier.hasVertex(src) && condition(dst)) resultReceiver(dst)
        }
      }
      resultReceiver
    }
    step(topDown, bottomUp)
  }

  class GroupBy[T](aggregate: (Long, T) => T, initial: T) extends VertexResultReceiver {

    val resultMap = new scala.collection.mutable.HashMap[Long, T]()

    def apply(vid: Long) : Unit = {
      this.synchronized {
        val curval : T = resultMap.getOrElse(vid, initial)
        resultMap.update(vid, aggregate(vid, curval))
      }
    }

    def results: Map[Long, T] = resultMap.toMap[Long, T]

    def -=(frontier: VertexFrontier) = {
      val dset = frontier.toSet
      resultMap --= dset
      this
    }
  }


  def groupByCount = new GroupBy[Int]((Long, cursum: Int) => cursum + 1, 0)

}
