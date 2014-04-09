package edu.cmu.graphchi.shards;

/**
 * Custom iterator that avoids object creation
 * @author Aapo Kyrola
 */
public interface EdgeIterator {

    boolean hasNext();

    void next();

    long getSrc();

    long getDst();

    // The index of the current edge
    int getIdx();

    public byte getType();

}
