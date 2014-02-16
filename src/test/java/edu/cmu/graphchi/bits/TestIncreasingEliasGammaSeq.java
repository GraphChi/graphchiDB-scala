package edu.cmu.graphchi.bits;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Aapo Kyrola
 */
public class TestIncreasingEliasGammaSeq {

    @Test
    public void testSmall() {
        long[] orig = new long[] {0, 9, 13, 19, 34,35,36,100,10000,10002,10004};

        IncreasingEliasGammaSeq egSeq = new IncreasingEliasGammaSeq(orig);

        for(int i=0; i < orig.length; i++) {
            long x = egSeq.get(i);
            System.out.println(i + " : " + x);
            assertEquals(orig[i], x);
        }

    }

    @Test
    public void testBig() {
        long[] orig = new long[10000000];

        long t1 = System.currentTimeMillis();
        orig[0] = 0;
        Random r = new Random();
        for(int i=1; i<orig.length; i++) {
            long delta = 1 +  Math.abs(r.nextLong() % 100);
            orig[i] = orig[i - 1] + delta;
        }

        long t2 = System.currentTimeMillis();

        IncreasingEliasGammaSeq egSeq = new IncreasingEliasGammaSeq(orig);

        for(int i=0; i < orig.length; i++) {
            long x = egSeq.get(i);
            assertEquals(orig[i], x);
        }

        long t3 = System.currentTimeMillis();
        System.out.println("Encoding took " + (t2 - t1) + " ms, while reading took " + (t3 - t2) + " ms" + " = " +
                ((t3 - t2) * 1.0) / orig.length + " ms / element");
    }
}
