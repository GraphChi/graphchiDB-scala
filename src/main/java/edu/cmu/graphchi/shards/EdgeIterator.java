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

    public byte getType();

}
