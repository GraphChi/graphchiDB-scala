package edu.cmu.graphchidb.queries.frontier
import org.junit.Test
import org.junit.Assert._
import edu.cmu.graphchidb.{DatabaseIndexing, Util}
import scala.util.Random

/**
 * @author Aapo Kyrola
 */
class TestFrontier {

  val vertexIndexing = new DatabaseIndexing {
    def shardForIndex(idx: Long) = (idx / 10000).toInt

    def shardSize(shardIdx: Int) = 10000

    def name = "test"

    def globalToLocal(idx: Long) = (idx % 10000).toInt

    def nShards = 128
  }

  @Test def testDenseAndSparseFrontier() = {
    val maxId = vertexIndexing.nShards * vertexIndexing.shardSize(0)

    // Make random set
    val r = new Random(260379)
    val n = 324231

    val idsToSet = (0 until n).map(i => math.abs(r.nextLong() % maxId)).toSet
    val setSize = idsToSet.size

    println("Setsize: %d".format(setSize))

    val denseFrontier1 = new DenseVertexFrontier(vertexIndexing)
    val sparseFrontier1 = new SparseVertexFrontier(vertexIndexing)


    List(denseFrontier1, sparseFrontier1).foreach( frontier => {
      println("Test frontier: %s", frontier)
      assertEquals(0, frontier.size)
      assertTrue(frontier.isEmpty)
      idsToSet.foreach(id => frontier.insert(id))

      assertEquals(setSize, frontier.size)
      assertFalse(frontier.isEmpty)

      idsToSet.foreach(id => assertTrue(frontier.hasVertex(id)))

      for(i <- 0 until n) {
        val id = math.abs(r.nextLong() % maxId)
        assertEquals(idsToSet.contains(id), frontier.hasVertex(id))
      }
    })
  }

}
