package edu.cmu.graphchidb.storage

import org.junit.Test
import org.junit.Assert._
import java.io.File

import edu.cmu.graphchidb.storage.ByteConverters._
import scala.util.Random

/**
 * @author Aapo Kyrola
 */
class TestDenseStorage {

  val testFile = new File("/tmp/graphchidbtest1")
  testFile.deleteOnExit()


  @Test def testMmappedDenseStorage() {
    val storage = new MemoryMappedDenseByteStorageBlock(testFile, 10000, 4) with DataBlock[Int]

    Random.shuffle((0 until 10000).toList).foreach(i => storage.set(idx = i, value = i * 2))

    Random.shuffle((0 until 10000).toList).foreach(i => {
      val x = storage.get(i)
      assertEquals(i * 2, x)
    }
    )

  }
}

