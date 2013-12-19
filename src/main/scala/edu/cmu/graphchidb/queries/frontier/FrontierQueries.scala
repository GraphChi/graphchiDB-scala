package edu.cmu.graphchidb.queries.frontier

import edu.cmu.graphchidb.GraphChiDatabase

/**
 * @author Aapo Kyrola
 */
object FrontierQueries {

  type queryFunction = (GraphChiDatabase, VertexFrontier) => VertexFrontier

  def outNeighbors(edgeType: Byte) : queryFunction = {
    def topDown(db: GraphChiDatabase, frontier:SparseVertexFrontier) : VertexFrontier = {
         println("Top down: %d".format(frontier.size))
         val edgeList = db.queryOutMultiple(frontier.toSeq, edgeType)
         VertexFrontier.createFrontier(edgeList.getInternalIds, db)
    }

    def bottomUp(db: GraphChiDatabase, frontier:DenseVertexFrontier) : VertexFrontier  = {
        println("Bottom up: %d".format(frontier.size))
        val outFrontier = new DenseVertexFrontier(db.vertexIndexing)
        db.sweepAllEdges() (
          (src: Long, dst: Long, eType: Byte ) => {
             if (edgeType == eType && frontier.hasVertex(src)) outFrontier.insert(dst)
          }
       )
        outFrontier
    }

    (db: GraphChiDatabase, frontier : VertexFrontier) => {
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
        if (frontierSize < db.numShards * 2) {
           topDown(db, sparseFrontier)
        } else {
           bottomUp(db, denseFrontier)
        }
    }
  }

}
