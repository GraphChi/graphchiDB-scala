package edu.cmu.graphchidb.storage

import org.junit.Test
import org.junit.Assert._

import java.io.File
import edu.cmu.graphchidb.{GraphChiDatabaseAdmin, GraphChiDatabase}


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

    (0 to 1000000).foreach(i => {
      val id =  varDataCol.insert("testdata%d".format(i).getBytes)
      assertEquals(id, i)
      // Test read
      val retrieved = varDataCol.get(id)
      assertEquals("testdata%d".format(i), new String(retrieved))
    })

    (0 to 1000000).foreach(i => {
      val retrieved = varDataCol.get(i)
      assertEquals("testdata%d".format(i), new String(retrieved))
    })
  }

}