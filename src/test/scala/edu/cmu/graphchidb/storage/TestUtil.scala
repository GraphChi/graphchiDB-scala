package edu.cmu.graphchidb.storage
import org.junit.Test
import org.junit.Assert._
import edu.cmu.graphchidb.Util

/**
 * @author Aapo Kyrola
 */
class TestUtil {

  @Test def testBitSet() = {
    val x = 0L
    val y = Util.setBit(x, 35)

    (0 until 34).foreach(i => assertFalse(Util.getBit(x, i)))
    (0 until 34).foreach(i => assertFalse(Util.getBit(y, i)))

    assertTrue(Util.getBit(y, 35))
    (36 until 63).foreach(i => assertFalse(Util.getBit(y, i)))

    (0 until 63).foreach(i => assertTrue(Util.getBit(Util.setBit(x, i), i)))

    val z = Util.setBit(Util.setBit(Util.setBit(0L, 1), 17), 63)

    assertFalse(Util.getBit(z, 0))
    assertTrue(Util.getBit(z, 1))
    assertTrue(Util.getBit(z, 17))
    assertTrue(Util.getBit(z, 63))

  }

  @Test def intConversions() = {
    (0 until 10000).foreach( x => {
        val a = x * 999
        assertEquals(a, Util.intFromByteArray(Util.intToByteArray(a)))
    })
  }

}
