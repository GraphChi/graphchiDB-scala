package edu.cmu.graphchidb.compute

import org.junit.Test
import org.junit.Assert._

import edu.cmu.graphchidb.DatabaseIndexing

/**
 * @author Aapo Kyrola
 */
class TestBitsetScheduler {

  val vertexIndexing = new DatabaseIndexing {
    def shardForIndex(idx: Long) = (idx / 10000).toInt

    def shardSize(shardIdx: Int) = 10000

    def name = "vertex"

    def globalToLocal(idx: Long) = (idx % 10000).toInt

    def localToGlobal(shardIdx: Int, localIdx: Long) = shardIdx * 10000 + localIdx

    def nShards = 128
  }

  @Test def testBitSetScheduler() = {
      val bs = new BitSetScheduler(vertexIndexing)

      assertFalse(bs.isScheduled(5000))

      bs.addTask(5000)
      assertFalse(bs.isScheduled(5000))
      bs.addTask(5000, alreadyThisIteration = true)
      assertTrue(bs.isScheduled(5000))

      assertFalse(bs.isScheduled(2000))
      bs.addTaskToAll()
      assertTrue(bs.isScheduled(2000))
      assertTrue(bs.isScheduled(0))
      assertTrue(bs.isScheduled(128*10000 - 1))

  }
}
