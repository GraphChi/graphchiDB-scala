package edu.cmu.graphchi.bits;

/**
 * Stores an increasing sequence efficiently
 * @author Aapo Kyrola
 */
public class IncreasingEliasGammaSeq {

    private int length;

    public IncreasingEliasGammaSeq(long[] original) {
        length = original.length;

    }


    public long get(int idx) {
        return 0;
    }


    public int length() {
        return length;
    }

    public int sizeInBytes() {
        return -1;
    }


}
