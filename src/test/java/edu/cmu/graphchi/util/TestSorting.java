package edu.cmu.graphchi.util;

import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * @author Aapo Kyrola
 */
public class TestSorting {

    @Test
    public void testSortWithValues() {
        long[] ids = new long[] {7,4,5,8,2,9};
        float[] valuef = new float[] {7.0f, 4.0f, 5.0f, 8.0f, 2.0f, 9.0f};
        byte[] valuedat = new byte[4 * valuef.length];

        FloatConverter floatConv = new FloatConverter();
        for(int i=0; i < valuef.length; i++) {
            byte[] tmp = new byte[4];
            floatConv.setValue(tmp, valuef[i]);
            System.arraycopy(tmp, 0, valuedat, i * 4, 4);
        }

        Sorting.sortWithValues(ids, valuedat, 4);

        for(int i=0; i < valuef.length; i++) {
            byte[] tmp = new byte[4];
            System.arraycopy(valuedat, i * 4, tmp, 0, 4);
            float f = floatConv.getValue(tmp);
            assertEquals(ids[i] * 1.0f, f);
            assertTrue(i == 0 || ids[i] > ids[i-1]);
        }
    }

    @Test
    public void testLongIntSort() {
        int N = 10000000;
        long[] a = new long[N];

        Random r = new Random(260379);

        long achecksum = 0;
        for(int i=0; i<N; i++) {
            a[i] = Math.abs(r.nextLong() % (N / 4));
            achecksum += a[i];
        }

        long st = System.currentTimeMillis();
        int[] b = Sorting.radixSortWithIndex(a);
        System.out.println("Native radix sorting took: " + (System.currentTimeMillis() - st) + " ms");

        long idxsum = 0;
        long asum = 0;
        for(int i=0; i<N-1; i++) {
            assertTrue((a[i] < a[i+1]) || (a[i] == a[i+1] && b[i] <= b[i + 1]));
            assertTrue(b[i] >= 0 && b[i] < a.length);
            idxsum += b[i];
            asum += a[i];
        }
        asum += a[N-1];
        idxsum += b[N - 1];
        assertEquals(achecksum, asum);
        assertEquals((long)N*((long)N-1) / 2, idxsum);
    }

    /*
    @Test
    public void testManyJNISorts() {
        // To test memory leakage
        for(int i=0; i<1000; i++) {
            testLongIntSort();
            System.out.println(Runtime.getRuntime().freeMemory());
        }
    } */

    @Test
    public void testLongIntSortJavaWithIndex() {
        int N = 10000000;
        long[] a = new long[N];

        Random r = new Random(260379);

        for(int i=0; i<N; i++) {
            a[i] =  Math.abs(r.nextLong() % (N / 4));
        }

        long st = System.currentTimeMillis();
        int[] b = Sorting.quickSortWithIndexJava(a);
        System.out.println("Java sorting took: " + (System.currentTimeMillis() - st) + " ms");

        long idxsum = 0;
        for(int i=0; i<N-1; i++) {
            assertTrue((a[i] < a[i+1]) || (a[i] == a[i+1] && b[i] <= b[i + 1]));
            assertTrue(b[i] >= 0 && b[i] < a.length);
            idxsum += b[i];
        }
        idxsum += b[N - 1];
        assertEquals((long)N*((long)N-1) / 2, idxsum);
    }


    IntConverter intc = new IntConverter();

    @Test
    public void testMerge2WithValues() {


        /* Array 1 */
        long[] src1 = new long[] {1, 2 , 4, 5, 5, 5, 10};
        long[] dst1 = new long[] {1001, 1002, 1004, 5, 4005, 5005, 1010};
        byte[] values1 =  new byte[4 * src1.length];
        int checksum = 0;
        for(int i=0; i<src1.length; i++) {
            byte[] tmp = new byte[4];
            intc.setValue(tmp, (int)dst1[i]);
            System.arraycopy(tmp, 0, values1, i * 4, 4);
            checksum += src1[i];
        }

        /* Array 2 */
        long[] src2 = new long[] {2, 3, 5, 8, 9, 11, 20};
        long[] dst2 = new long[] {2002, 2003, 2005, 2008, 2009, 2011, 2020};
        byte[] values2 =  new byte[4 * src2.length];
        for(int i=0; i<src2.length; i++) {
            byte[] tmp = new byte[4];
            intc.setValue(tmp, (int)dst2[i]);
            System.arraycopy(tmp, 0, values2, i * 4, 4);
            checksum += src2[i];
        }

        /* Results */
        long[] ressrc = new long[src1.length + src2.length];
        long[] resdst = new long[src1.length + src2.length];
        byte[] resvals = new byte[values1.length + values2.length];

        Sorting.mergeWithValues(src1, dst1, values1, src2, dst2, values2, ressrc, resdst, resvals, 4);

        for(int j=1; j<ressrc.length; j++) {
            assertTrue(ressrc[j] >= ressrc[j-1]);
            assertTrue(resdst[j] % 1000 == ressrc[j]);
            if (ressrc[j] == ressrc[j-1]) assertTrue(resdst[j] > resdst[j-1]);

            byte[] tmp = new byte[4];
            System.arraycopy(resvals, j * 4, tmp, 0, 4);
            int x = intc.getValue(tmp);
            assertEquals(resdst[j], (long)x);
        }

        // Merge other direction
        Sorting.mergeWithValues( src2, dst2, values2, src1, dst1, values1, ressrc, resdst, resvals, 4);

        for(int j=1; j<ressrc.length; j++) {
            assertTrue(ressrc[j] >= ressrc[j-1]);
            assertTrue(resdst[j] % 1000 == ressrc[j]);

            if (ressrc[j] == ressrc[j-1]) assertTrue(resdst[j] > resdst[j-1]);

            byte[] tmp = new byte[4];
            System.arraycopy(resvals, j * 4, tmp, 0, 4);
            int x = intc.getValue(tmp);
            assertEquals(resdst[j], (long)x);
        }


        /* Check that all values in */
        int sum = 0;
        for(int i=0; i<ressrc.length; i++) sum += ressrc[i];
        assertEquals(checksum, sum);

    }

    @Test
    public void testMergeLarge() {
        long[] src1 = new long[100000];
        long[] dst1 = new long[100000];

        for(int j=0; j<src1.length; j++) {
            src1[j] = (j / 4) * 9999;
            dst1[j] = src1[j] + 1;
        }

        byte[] values1 =  new byte[4 * src1.length];
        int checksum = 0;
        for(int i=0; i<src1.length; i++) {
            byte[] tmp = new byte[4];
            intc.setValue(tmp, (int)dst1[i]);
            System.arraycopy(tmp, 0, values1, i * 4, 4);
            checksum += src1[i];
        }

        /* Array 2 */
        long[] src2 = new long[153003];
        long[] dst2 =  new long[153003];
        for(int j=0; j<src2.length; j++) {
            src2[j] =  (j / 7) * 9999;
            dst2[j] = src2[j] + 1;
        }
        byte[] values2 =  new byte[4 * src2.length];
        for(int i=0; i<src2.length; i++) {
            byte[] tmp = new byte[4];
            intc.setValue(tmp, (int)dst2[i]);
            System.arraycopy(tmp, 0, values2, i * 4, 4);
            checksum += src2[i];
        }

          /* Results */
        long[] ressrc = new long[src1.length + src2.length];
        long[] resdst = new long[src1.length + src2.length];
        byte[] resvals = new byte[values1.length + values2.length];

        Sorting.mergeWithValues(src1, dst1, values1, src2, dst2, values2, ressrc, resdst, resvals, 4);

        for(int j=1; j<ressrc.length; j++) {
            assertTrue((ressrc[j] >= ressrc[j-1]|| (ressrc[j-1] == ressrc[j] && resdst[j] > resdst[j-1] )));
            if (ressrc[j] == ressrc[j-1] && resdst[j] < resdst[j-1]) {
                System.out.println("Mismatch: "+ j);
                System.out.println(ressrc[j]);
                System.out.println(resdst[j-1]);
                System.out.println(resdst[j]);

                assertTrue(resdst[j] > resdst[j-1]);
            }

            byte[] tmp = new byte[4];
            System.arraycopy(resvals, j * 4, tmp, 0, 4);
            int x = intc.getValue(tmp);
            assertEquals(resdst[j], (long)x);
        }


        // Merge other direction
        Sorting.mergeWithValues( src2, dst2, values2, src1, dst1, values1, ressrc, resdst, resvals, 4);

        for(int j=1; j<ressrc.length; j++) {
            assertTrue((ressrc[j] >= ressrc[j-1]|| (ressrc[j-1] == ressrc[j] && resdst[j] > resdst[j-1] )));

            if (ressrc[j] == ressrc[j-1] && resdst[j] < resdst[j-1]) {
                System.out.println("Mismatch: "+ j);
                System.out.println(ressrc[j]);
                System.out.println(resdst[j-1]);
                System.out.println(resdst[j]);

                assertTrue(resdst[j] > resdst[j-1]);
            }
            byte[] tmp = new byte[4];
            System.arraycopy(resvals, j * 4, tmp, 0, 4);
            int x = intc.getValue(tmp);
            assertEquals(resdst[j], (long)x);
        }
    }
}
