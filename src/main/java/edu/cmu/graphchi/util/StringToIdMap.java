package edu.cmu.graphchi.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Utility for efficiently mapping strings to IDs. Based on hashing
 * and handles collisions. Note: this data structure assumes that existence
 * of a key is known beforehand.
 * @author Aapo Kyrola
 */
public class StringToIdMap {


     int[] idTable;
    long count = 0;
    long numCollisions = 0;

    HashMap<Integer, LinkedList<IdString>> collisions = new HashMap<>();

    public StringToIdMap(int hashtableSizeEntries) {
        idTable = new int[hashtableSizeEntries];
        Arrays.fill(idTable, -1);
    }

    private int hash(String s) {
        return Math.abs(s.hashCode() % idTable.length);
    }

    /**
     * Note: s must be unique, i.e not inserted before!
     * @param s
     * @return
     */
    public void put(String s, int id) {
        count++;
        int h = hash(s);
        if (idTable[h] == -1) {
           idTable[h] = id;
        } else {
            if (collisions.get(h) == null) {
                collisions.put(h, new LinkedList<IdString>());
            }
            numCollisions++;
            collisions.get(h).add(new IdString(id, s));
            if (numCollisions % 10000 == 0) System.out.println("Collision entries: " + numCollisions + "total:" + count);
        }
    }

    /**
     * Note: it is assumed s exists in the table!
     * @param s
     * @return
     */
    public int getId(String s) {
        int h = hash(s);
        LinkedList<IdString> collision = collisions.get(h);
        if (collision == null) {
            return idTable[h];
        } else {
            for(IdString idstr : collision) {
                if (idstr.str.equals(s)) {
                    return idstr.id;
                }
            }
            // Not in collision list, so is the original
            return idTable[h];
        }
    }


    static class IdString {
        private int id;
        private String str;

        IdString(int id, String str) {
            this.id = id;
            this.str = str;
        }
    }
}
