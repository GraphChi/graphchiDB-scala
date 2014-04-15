package edu.cmu.graphchi.util;

import java.util.*;

/**
 * Utility for efficiently mapping strings to IDs. Based on hashing
 * and handles collisions. Note: this data structure assumes that existence
 * of a key is known beforehand.
 * @author Aapo Kyrola
 */
public class StringToIdMap {

    public ArrayList<IdString> strings;
    private boolean finalized = false;

    public StringToIdMap(int initialSize) {
        strings = new ArrayList<>(initialSize);
    }

    public void put(String s, int id) {
        strings.add(new IdString(id, s));
    }


    public void compute() {
        System.out.println("Finalizing id mapping. Index contains " + strings.size() + " strings.");
        Collections.sort(strings);
        System.out.println("Done finalizing id mapping.");

        finalized = true;
    }


    public int getId(String s) {
        if (!finalized) throw new IllegalStateException("StringToIdMap not finalized");

        int idx = Collections.binarySearch(strings, new IdString(0, s));
        if (idx < 0) return -1;
        else return strings.get(idx).id;
    }

    static class IdString implements Comparable<IdString> {
        private int id;
        private String str;

        IdString(int id, String str) {
            this.id = id;
            this.str = str;
        }

        @Override
        public int compareTo(IdString o) {
            return this.str.compareTo(o.str);
        }
    }
}
