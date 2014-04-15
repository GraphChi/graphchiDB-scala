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
package edu.cmu.graphchi.bits;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;
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

    @Test
    public void testBigTwo() {
        long[] orig = new long[10000000];

        orig[0] = 0;
        Random r = new Random();
        for(int i=1; i<orig.length; i++) {
            long delta = 1 +  Math.abs(r.nextLong() % 100);
            orig[i] = orig[i - 1] + delta;
        }

        long t1 = System.currentTimeMillis();

        IncreasingEliasGammaSeq egSeq = new IncreasingEliasGammaSeq(orig);
        long t2 = System.currentTimeMillis();

        for(int i=0; i < orig.length - 1; i++) {
            long[] x = egSeq.getTwo(i);
            assertEquals(orig[i], x[0]);
            assertEquals(orig[i + 1], x[1]);
        }

        long t3 = System.currentTimeMillis();
        System.out.println("Encoding took " + (t2 - t1) + " ms, while reading took " + (t3 - t2) + " ms" + " = " +
                ((t3 - t2) * 1.0) / orig.length + " ms / element");
    }

    @Test
    public void testFind() {
        long[] orig = new long[1000000];

        orig[0] = 0;
        Random r = new Random();
        for(int i=1; i<orig.length; i++) {
            long delta = 1 +  Math.abs(r.nextLong() % 100);
            orig[i] = orig[i - 1] + delta;
        }

        ArrayList<Long> evens = new ArrayList<>(orig.length / 2);
        for(int i=0; i<orig.length / 2; i++) evens.add(orig[i * 2]);   // everysecond
        ArrayList<Long> odds = new ArrayList<>(orig.length / 2);
        for(int i=0; i<orig.length / 2; i++) odds.add(orig[i * 2 + 1]);   // everysecond

        long t1 = System.currentTimeMillis();

        IncreasingEliasGammaSeq egSeq = new IncreasingEliasGammaSeq(orig);

        long t2 = System.currentTimeMillis();
        int j;

        for(int i=0; i < orig.length - 1; i++) {
            j = egSeq.getIndex(orig[i]);
            assertEquals(i, j);
        }



    }



    @Test
    public void testIterator() {
        long[] orig = new long[10000000];

        long t1 = System.currentTimeMillis();
        orig[0] = 0;
        Random r = new Random(19820904);
        for(int i=1; i<orig.length; i++) {
            long delta = 1 +  Math.abs(r.nextLong() % 100);
            orig[i] = orig[i - 1] + delta;
        }

        long t2 = System.currentTimeMillis();

        IncreasingEliasGammaSeq egSeq = new IncreasingEliasGammaSeq(orig);
        Iterator<Long> iter = egSeq.iterator();
        int j = 0;
        while(iter.hasNext()) {
            assertTrue(iter.hasNext());
            long x = iter.next();
            assertEquals(orig[j], x);
            j++;
        }
        assertEquals(j, orig.length);

    }

    @Test
    public void testIteratorWithStart() {
        long[] orig = new long[1000000];

        long t1 = System.currentTimeMillis();
        orig[0] = 0;
        Random r = new Random(19820904);
        for(int i=1; i<orig.length; i++) {
            long delta = 1 +  Math.abs(r.nextLong() % 100);
            orig[i] = orig[i - 1] + delta;
        }

        long t2 = System.currentTimeMillis();

        IncreasingEliasGammaSeq egSeq = new IncreasingEliasGammaSeq(orig);

        for(int tries=0; tries<50; tries++) {
            boolean shifted = false;

            int j = Math.abs(r.nextInt() % orig.length);
            int startj = j;
            long st = orig[startj];
            if (startj > 0 && orig[startj - 1] < st - 1) {
                st--; // Shifted
                shifted = true;
            }
            Iterator<Long> iter = egSeq.iterator(st);
            while(iter.hasNext()) {
                assertTrue(iter.hasNext());
                long x = iter.next();
                if (j >= orig.length || orig[j] != x) {
                   System.err.println("Mismatch: j =" + j + "/" + orig.length + ", startj=" + startj + ", try=" + tries);
                   System.out.println("shifted: " + shifted);
                }
                assertEquals(orig[j], x);
                j++;
            }
            assertEquals(j, orig.length);
        }
    }
}
