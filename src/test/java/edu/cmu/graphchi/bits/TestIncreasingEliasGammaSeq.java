package edu.cmu.graphchi.bits;

import org.junit.Test;

/**
 * @author Aapo Kyrola
 */
public class TestIncreasingEliasGammaSeq {

    @Test
    public void testBasics() {
        long[] orig = new long[] {0, 9, 13, 19, 34,35,36,100,10000,10002,10004};

        IncreasingEliasGammaSeq egSeq = new IncreasingEliasGammaSeq(orig);

        for(int i=0; i < orig.length; i++) {

        }

    }


}
