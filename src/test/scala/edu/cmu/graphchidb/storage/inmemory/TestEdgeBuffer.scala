package edu.cmu.graphchidb.storage

import org.junit.Test
import org.junit.Assert._

import java.io.File
import edu.cmu.graphchidb.{DecodedEdge, GraphChiDatabaseAdmin, GraphChiDatabase}
import edu.cmu.graphchidb.Util.timed
import edu.cmu.graphchidb.storage.inmemory.EdgeBuffer
import scala.util.Random
import java.nio.ByteBuffer

/**
 * @author Aapo Kyrola
 */
class TestEdgeBuffer {

  val dir = new File("/tmp/graphchidbtest")
  dir.mkdir()
  dir.deleteOnExit()

  val testDb = "/tmp/graphchidbtest/test1"

  GraphChiDatabaseAdmin.createDatabase(testDb, 2)

  case class TestEdge(src: Long, dst: Long, col1: String, col2: Int, col3: Int) {
     def isValid =  (col2 == (src + dst) % 10000) && (col3 == (src - dst) % 333 && col1 == "c")
  }

  val db = new GraphChiDatabase(testDb, 2)
  val catColumn = db.createCategoricalColumn("col1", IndexedSeq("a", "b", "c"), db.edgeIndexing)
  db.createIntegerColumn("col2", db.edgeIndexing)
  db.createIntegerColumn("col3", db.edgeIndexing)

  val eed = db.edgeEncoderDecoder

  /* Create edge buffer */
  val edgeBuffer = new EdgeBuffer(eed, 1000)

  /* Create test set of edges */


  val edgesToCreate = Random.shuffle( (0 until 1000).map(i => {
    (0 until 4).map( j => {
      val src = i
      val dst = i + j
      TestEdge(src, dst, "c", (src + dst) % 10000, (src - dst) % 333)
    } ) } ).flatten.toSeq )

  println("Creating %d edges".format(edgesToCreate.size))
  edgesToCreate.foreach(edge => edgeBuffer.addEdge(edge.src, edge.dst, catColumn.indexForName(edge.col1), edge.col2, edge.col3))





  @Test def testEdgeBuffer = {
     assertEquals(edgeBuffer.numEdges, edgesToCreate.size)

    def fromDecodedEdge(dec: DecodedEdge) = TestEdge(dec.src, dec.dst, catColumn.categoryName(dec.values(0).asInstanceOf[Byte]),
        dec.values(1).asInstanceOf[Int], dec.values(2).asInstanceOf[Int])

    /* Do searches */
    var totalOut = 0

    timed("out", {
    (0 until 1000).foreach(src => {
        val results = edgeBuffer.findOutNeighborsEdges(src).toSet
        totalOut += results.size
        assertEquals(4, results.size)
        results.foreach(r => assertEquals(src, r.src))
        results.foreach(r => assertEquals(true, fromDecodedEdge(r).isValid))
    }) }
    )
    var totalIn = 0
    timed("in", {
    (0 until 1003).foreach(dst => {
      val results = edgeBuffer.findInNeighborsEdges(dst).toSet
      assertTrue(results.size > 0)
      totalIn += results.size
      results.foreach(r => assertEquals(dst, r.dst))
      results.foreach(r => assertEquals(true, fromDecodedEdge(r).isValid))
    }) })

    assertEquals(4000, totalOut)
    assertEquals(4000, totalIn)

    timed("not-found-out", {
       val results = edgeBuffer.findOutNeighborsEdges(99999)
       assertEquals(0, results.size)
    })

    timed("not-found-in", {
      val results = edgeBuffer.findOutNeighborsEdges(99999)
      assertEquals(0, results.size)
    })

  }

  @Test def testProjection = {
    val checkSumColumn2 = edgesToCreate.zipWithIndex.map(e => e._1.col2.toLong * (1 + e._2)).sum
    val checkSumColumn3 = edgesToCreate.zipWithIndex.map(e => e._1.col3.toLong * (1 + e._2)).sum
    printf("Checksums: %d, %d\n".format(checkSumColumn2, checkSumColumn3))

    val col2Buffer = ByteBuffer.allocate(edgesToCreate.size * 4)
    val col3Buffer = ByteBuffer.allocate(edgesToCreate.size * 4)

    timed("projection-1", {
      edgeBuffer.projectColumnToBuffer(1, col2Buffer)   })
    timed("projection-2", {
      edgeBuffer.projectColumnToBuffer(2, col3Buffer)   })

    assertEquals(0, col2Buffer.position())
    val col2IntsBuf = col2Buffer.asIntBuffer()
    col2IntsBuf.rewind()
    val col2Ints = new Array[Int](edgesToCreate.size)
    printf("col2IntsBuf size: %d, orig: %d\n".format(col2IntsBuf.capacity(), col2Buffer.capacity()))
    col2IntsBuf.get(col2Ints)
    val col3IntsBuf = col3Buffer.asIntBuffer()
    col3IntsBuf.rewind()
    val col3Ints = new Array[Int](edgesToCreate.size)
    col3IntsBuf.get(col3Ints)

    (0 until 10).foreach(i => printf("%d = %d / %d\n", i, col2Ints(i), edgesToCreate(i).col2))

    val checkSum2test = col2Ints.zipWithIndex.map(e => e._1.toLong * (1 + e._2)).sum
    val checkSum3test = col3Ints.zipWithIndex.map(e => e._1.toLong * (1 + e._2)).sum

    assertEquals(checkSumColumn2, checkSum2test)
    assertEquals(checkSumColumn3, checkSum3test)
  }
}
