package edu.cmu.graphchidb.examples.computation

 import edu.cmu.graphchidb.GraphChiDatabase
import edu.cmu.graphchidb.compute.{GraphChiContext, GraphChiVertex, VertexCentricComputation}
import edu.cmu.graphchidb.storage.{Column, ByteConverter}
import java.nio.ByteBuffer

import org.apache.commons.math.linear._

/**
 * Matrix factorization with the Alternative Least Squares (ALS) algorithm.
 * This code is based on GraphLab's implementation of ALS by Joey Gonzalez
 * and Danny Bickson (CMU). A good explanation of the algorithm is
 * given in the following paper:
 *    Large-Scale Parallel Collaborative Filtering for the Netflix Prize
 *    Yunhong Zhou, Dennis Wilkinson, Robert Schreiber and Rong Pan
 *    http://www.springerlink.com/content/j1076u0h14586183/
 *
 *
 * This version stores the latent factors in memory and thus requires
 * sufficient memory to store D floating point numbers for each vertex.
 * D is a dimensionality factor (default 5).
 *
 * Each edge stores a "rating" and the purpose of this algorithm is to
 * find a matrix factorization U x V so that U x V approximates the rating
 * matrix R.
 *
 * This application reads Matrix Market format (similar to the C++ version)
 * and outputs the latent factors in two files in the matrix market format.
 * To test, you can download small-netflix data from here:
 * http://select.cs.cmu.edu/code/graphlab/smallnetflix_mme
 *
 * <i>Note:</i>  in this case the vertex values are not used, but as GraphChi does
 * not currently support "no-vertex-values", integer-type is used as placeholder.
 *
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, 2014
 */

// D-dimensional type. Unfortunately, it is hard to specify variable size types, so the dimension is hard
// coded

object FactorVec {
  val D = 10

  /** Converter that converts bytes to factors. Necessary evil... */
  implicit object FactorVecByteConverter extends ByteConverter[FactorVec] {
    def fromBytes(bb: ByteBuffer) = {
       val fv = new FactorVec
       var i = 0
       while(i < D) {
          fv.values(i) = bb.getDouble(i)
          i += 1
       }
       fv
    }

    def toBytes(fv: FactorVec, out: ByteBuffer) = {
      var i = 0
      while(i < D) {
         out.putDouble(fv.values(i))
         i += 1
      }
    }

    def sizeOf = 8 * D
  }
}

class FactorVec {
    val values: Array[Double] = new Array[Double](FactorVec.D)
    def randomize = {
        var i = 0
        while(i < values.length) {
            values(i) = math.random
            i += 1
        }
        this
    }

    def this(rv: RealVector) {
      this()
      var i = 0
      while(i < values.length) {
        values(i) = rv.getEntry(i)
        i += 1
      }
    }

    def dot(other: FactorVec) = {
      var i = 0
      var x = 0
      while(i < values.length) {
        x += values(i) * other.values(i)
        i += 1
      }
      x
    }

    def apply(i: Int) = values(i)
}



/**
 * @author Aapo Kyrola
 */
class ALSMatrixFactorization(factorColumnName: String, ratingColumn: Column[Byte], database: GraphChiDatabase) extends VertexCentricComputation[FactorVec, Byte]{
  private val factorColumn = database.createCustomTypeColumn[FactorVec](factorColumnName, database.vertexIndexing, FactorVec.FactorVecByteConverter, false)
  factorColumn.autoFillVertexFunc =  Some((id: Long) => new FactorVec randomize)

  val D = FactorVec.D
  val LAMBDA = 0.065   // Regularization parameter

  /**
   * Update function to be implemented by an algorithm.
   * Direct translation from GraphChi-Java's ALSMatrixFactorization.java
   * @param vertex
   * @param context
   */
  def update(vertex: GraphChiVertex[FactorVec, Byte], context: GraphChiContext) : Unit = {
     if (vertex.getNumEdges == 0) return
     val XtX = new BlockRealMatrix(D, D)
     val Xty = new ArrayRealVector(D)



     vertex.edges.foreach(edge => {
         val observation = edge.getValue.toDouble
         val neighborLatent = factorColumn.get(edge.vertexId).get

         var i = 0
         while(i < D) { // Use ugly while loops for performance reasons
            Xty.setEntry(i, Xty.getEntry(i) + neighborLatent(i) * observation)
            var j = 1
            while(j < D) {
                XtX.setEntry(j, i, XtX.getEntry(j ,i) + neighborLatent(i) * neighborLatent(j))
                j += 1
              }
            i += 1
         }

         // Symmetrize
         i = 0
         while(i < D) {
            var j = i + 1
            while(j < D) {
                XtX.setEntry(i, j, XtX.getEntry(j, i))
                j += 1
            }
            i += 1
         }

         // Diagonal, add regularization
         i = 0
         while(i < D) {
             XtX.setEntry(i, i, XtX.getEntry(i, i) + LAMBDA * vertex.getNumEdges)
             i += 1
         }

        // Solve the least-squares optimization using Cholesky Decomposition
        val newLatent = new FactorVec(new CholeskyDecompositionImpl(XtX).getSolver.solve(Xty))
        factorColumn.set(vertex.id, newLatent)

     })
  }

  def edgeDataColumn = Some(ratingColumn)

  def vertexDataColumn = Some(factorColumn)

  def predictRating(v1: Long, v2: Long) = {
    val nbrLatent = factorColumn.get(v1).get
    val thisLatent = factorColumn.get(v2).get
    thisLatent.dot(nbrLatent)
  }

  def computeRMSE = {
      var rmse = 0
      var count = 0L
      database.sweepInEdgesWithJoin(ratingColumn)( (src:Long, dst:Long, edgeType: Byte, rating: Byte) => {
           val observation = rating.toDouble
           val prediction = predictRating(src, dst)
           rmse += (observation - prediction) * (observation - prediction)
           count += 1
      })
     (math.sqrt(rmse / count), count)
  }
}
