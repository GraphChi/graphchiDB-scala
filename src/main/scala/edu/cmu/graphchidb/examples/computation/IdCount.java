package edu.cmu.graphchidb.examples.computation;

import java.io.Serializable;

/**
 * @author Aapo Kyrola
 */
public class IdCount implements Comparable<IdCount>, Serializable {
    public long id;
    public int count;

    public IdCount(long id, int count) {
        this.id = id;
        this.count = count;
    }

    public int compareTo(IdCount idCount) {
        return (idCount.count > this.count ? 1 : (idCount.count != this.count ? -1 : (idCount.id < this.id ? -1 : 1)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdCount idCount = (IdCount) o;

        if (id != idCount.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (id % Integer.MAX_VALUE);
    }

    public String toString() {
        return id + ": " + count;
    }
}
