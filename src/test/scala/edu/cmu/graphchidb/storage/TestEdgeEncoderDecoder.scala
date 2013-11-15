package edu.cmu.graphchidb.storage

import org.junit.Test
import org.junit.Assert._

import java.io.File
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}
import java.nio.ByteBuffer

/**
 * @author Aapo Kyrola
 */
class TestEdgeEncoderDecoder {

  val dir = new File("/tmp/graphchidbtest")
  dir.mkdir()
  dir.deleteOnExit()

  val testDb = "/tmp/graphchidbtest/test1"

  GraphChiDatabaseAdmin.createDatabase(testDb, 2)

  @Test def testEdgeEncoderDecoder = {
      val db = new GraphChiDatabase(testDb, 2)
      val catColumn = db.createCategoricalColumn("col1", IndexedSeq("a", "b", "c"), db.edgeIndexing)
      db.createIntegerColumn("col2", db.edgeIndexing)
      db.createIntegerColumn("col3", db.edgeIndexing)

      val eed = db.edgeEncoderDecoder

      assertEquals(1 + 4 + 4, eed.edgeSize)

      val byteBuffer = ByteBuffer.allocate(eed.edgeSize)
      eed.encode(byteBuffer, catColumn.indexForName("b"), 10, 88888)

      byteBuffer.rewind

      val decoded = eed.decode(byteBuffer, 999, 7832742)

      println("decoded:" + decoded)

      assertEquals(999, decoded.src)
      assertEquals(7832742, decoded.dst)
      assertEquals("b", catColumn.categoryName(decoded.values(0).asInstanceOf[Byte]))
      assertEquals(10, decoded.values(1).asInstanceOf[Int])
      assertEquals(88888, decoded.values(2).asInstanceOf[Int])

  }
}
