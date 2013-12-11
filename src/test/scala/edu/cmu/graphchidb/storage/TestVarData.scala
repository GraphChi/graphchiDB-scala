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

    val varDataCol = db.createVarDataColumn("testvardata", db.vertexIndexing, null)

    val ids = new ArrayBuffer[Long]()

    (0 to 1000000).foreach(i => {
      val id =  varDataCol.insert("testdata%d".format(i).getBytes)
      // Test read

      val retrieved = varDataCol.get(id)
      ids += id
      assertEquals("testdata%d".format(i), new String(retrieved))
    })

    val t = System.currentTimeMillis()
    (0 to 1000000).foreach(i => {
      val retrieved = varDataCol.get(ids(i))
      assertEquals("testdata%d".format(i), new String(retrieved))
    })
    val dt = System.currentTimeMillis() - t
    println("Retrieval took %s ms, %s ms / search".format(dt, dt * 1.0 / 1000000.0))
  }

}