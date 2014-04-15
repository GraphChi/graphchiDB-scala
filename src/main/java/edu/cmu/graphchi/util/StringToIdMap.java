/**
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2014] [Aapo Kyrola / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Publication to cite:  http://arxiv.org/abs/1403.0701
 */
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
