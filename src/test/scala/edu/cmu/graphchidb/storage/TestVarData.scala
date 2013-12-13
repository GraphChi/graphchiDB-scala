package edu.cmu.graphchidb.storage

import org.junit.Test
import org.junit.Assert._

import java.io.File
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}
import scala.collection.mutable.ArrayBuffer


/**
 * @author Aapo Kyrola
 */
class TestVarData {
  val dir = new File("/tmp/graphchidbtest")
  dir.mkdir()
  dir.deleteOnExit()

  val testDb = "/tmp/graphchidbtest/test1"

  GraphChiDatabaseAdmin.createDatabase(testDb)

  @Test def testVarData() = {
    val db = new GraphChiDatabase(testDb)

    val varDataCol = db.createVarDataColumn("testvardata", db.vertexIndexing)
    varDataCol.maxFileSize = 100000

    val ids = new ArrayBuffer[Long]()

    (0 to 1000000).foreach(i => {
      val id =  varDataCol.insert("testdata%d".format(i).getBytes)
      // Test read

      val retrieved = varDataCol.get(id)
      ids += id
      assertEquals("testdata%d".format(i), new String(retrieved))

       // halfway
      val halfWayId = ids(i / 2)
      val retrieved2 = varDataCol.get(halfWayId)
      assertEquals("testdata%d".format(i / 2), new String(retrieved2))
    })

    var t = System.currentTimeMillis()
    (0 to 1000000).foreach(i => {
      val retrieved = varDataCol.get(ids(i))
      assertEquals("testdata%d".format(i), new String(retrieved))
    })
    var dt = System.currentTimeMillis() - t
    println("Retrieval took %s ms, %s ms / search".format(dt, dt * 1.0 / 1000000.0))
    varDataCol.flushBuffer()

    // Second retrieval, with recreated column
    val varDataCol2 = db.createVarDataColumn("testvardata", db.vertexIndexing)
    t = System.currentTimeMillis()
    (0 to 1000000).foreach(i => {
      val retrieved = varDataCol2.get(ids(i))
      assertEquals("testdata%d".format(i), new String(retrieved))
    })
    dt = System.currentTimeMillis() - t
    println("Retrieval-2 took %s ms, %s ms / search".format(dt, dt * 1.0 / 1000000.0))


  }

}