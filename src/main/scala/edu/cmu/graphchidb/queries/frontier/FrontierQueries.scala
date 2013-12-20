package edu.cmu.graphchidb.queries.frontier

import edu.cmu.graphchidb.GraphChiDatabase
import scala.collection.mutable

/**
 * @author Aapo Kyrola
 */
object FrontierQueries {

  type queryFunction = (VertexFrontier) => VertexFrontier


  def queryVertex(vid: Long)(implicit db:GraphChiDatabase) : VertexFrontier =
    new SparseVertexFrontier(db.vertexIndexing, Set(vid), db)

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
      if (frontierSize < frontier.db.numShards * 2) {
        sparseBlock(sparseFrontier)
      } else {
        denseBlock(denseFrontier)
      }
    }
  }

    /* Creates new frontier of the out-neighbors of the frontier */
  def traverseOut(edgeType: Byte) : queryFunction = {
    def topDown(frontier:SparseVertexFrontier) : VertexFrontier = {
         println("Top down: %d".format(frontier.size))
         val edgeList = frontier.db.queryOutMultiple(frontier.toSet, edgeType)
         VertexFrontier.createFrontier(edgeList.getInternalIds, frontier.db)
    }

    def bottomUp(frontier:DenseVertexFrontier) : VertexFrontier  = {
        println("Bottom up: %d".format(frontier.size))
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

  trait VertexResultReceiver {
     def apply(vid: Long) : Unit
  }


  /* Emits each out-neighbor for the frontier */
  def selectOut(edgeType: Byte, resultReceiver: VertexResultReceiver) = {
       def topDown(frontier: SparseVertexFrontier) : Unit = {
         println("Top down -- select: %d".format(frontier.size))
         val edgeList = frontier.db.queryOutMultiple(frontier.toSet, edgeType)
         edgeList.getInternalIds.foreach(vid => resultReceiver(vid))
       }
      def bottomUp(frontier: DenseVertexFrontier) : Unit = {
        println("Bottom up -- select: %d".format(frontier.size))
        frontier.db.sweepAllEdges() {
           (src: Long, dst: Long, eType: Byte ) => {
             if (edgeType == eType && frontier.hasVertex(src)) resultReceiver(dst)
           }
         }
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
  }


  def groupByCount() = new GroupBy[Int]((Long, cursum: Int) => cursum + 1, 0)

}
