package edu.cmu.graphchidb.storage

import org.junit.Test
import org.junit.Assert._
import java.io.File

import edu.cmu.graphchidb.storage.ByteConverters._
import scala.util.Random
import java.nio.{ByteBuffer, IntBuffer}

/**
 * @author Aapo Kyrola
 */
class TestDenseStorage {

  val testFile = new File("/tmp/graphchidbtest1")
  testFile.deleteOnExit()

  val storage = new MemoryMappedDenseByteStorageBlock(testFile, 10000, 4) with DataBlock[Int]

  @Test def testMmappedDenseStorage() {

    Random.shuffle((0 until 10000).toList).foreach(i => storage.set(idx = i, value = i * 2))

    Random.shuffle((0 until 10000).toList).foreach(i => {
      val x = storage.get(i).get
      assertEquals(i * 2, x)
    }
    )
  }

  @Test def testCreateNew() {
     val byteBuffer = ByteBuffer.allocate(1000 * 4)
     val intBuffer = byteBuffer.asIntBuffer()
     (0 until 1000).foreach(intBuffer.put)
     byteBuffer.rewind

     val arr =byteBuffer.array
     val newStorage = storage.createNew[Int](arr)
     assertEquals(1000, newStorage.size)

     (0 until 1000).foreach(i => assertEquals(i, newStorage.get(i).get))

  }

}

