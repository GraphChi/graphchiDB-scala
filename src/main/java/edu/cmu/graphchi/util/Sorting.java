package edu.cmu.graphchi.util;

import java.util.Random;

/**
 * @author Aapo Kyrola
 */
public class Sorting {

    private static final Random random = new Random();

    // http://www.algolist.net/Algorithms/Sorting/Quicksort
    // TODO: implement faster
    private static int partition(long arr[], long arr2[], byte[] values, int sizeOf, int left, int right)
    {
        int i = left, j = right;
        long tmp, tmp2;
        int pivotidx = left + random.nextInt(right - left + 1);
        long pivot1 = arr[pivotidx];
        long pivot2 = arr2[pivotidx];
        byte[] valueTemplate = new byte[sizeOf];

        while (i <= j) {
            while (arr[i] < pivot1 || (arr[i] == pivot1 && arr2[i] < pivot2))
                i++;
            while (arr[j] > pivot1 || (arr[j] == pivot1 && arr2[j] > pivot2))
                j--;
            if (i <= j) {
                tmp = arr[i];
                tmp2 = arr2[i];

                /* Swap */
                System.arraycopy(values, j * sizeOf, valueTemplate, 0, sizeOf);
                System.arraycopy(values, i * sizeOf, values, j * sizeOf, sizeOf);
                System.arraycopy(valueTemplate, 0, values, i * sizeOf, sizeOf);

                arr[i] = arr[j];
                arr[j] = tmp;
                arr2[i] = arr2[j];
                arr2[j] = tmp2;

                i++;
                j--;
            }
        }

        return i;
    }

    private static int partition(long arr[], int arr2[],  int left, int right)
    {
        int i = left, j = right;
        long tmp;
        int tmp2;
        int len = right - left + 1;
        int pivotidx1  = left + len / 4;
        int pivotidx2 = Math.min(left + 2 * len / 4, right - 1);
        int pivotidx3 = Math.min(left + 3 * len / 4, right - 1);

        // Take median
        long p1 = arr[pivotidx1];
        long p2 = arr[pivotidx2];
        long p3 = arr[pivotidx3];

        int pivotidx = pivotidx1;
        if (p1 <= p2 && p2 <= p3) {
            pivotidx = pivotidx2;
        } else if (p3 <= p2 && p2 <= p1) {
            pivotidx = pivotidx2;
        } else if (p2 <= p1 && p1 <= p3) {
            pivotidx = pivotidx1;
        } else if (p3 <= p1 && p1 <= p2) {
            pivotidx = pivotidx1;
        } else pivotidx = pivotidx3;


        long pivot1 = arr[pivotidx];
        int pivot2 = arr2[pivotidx];

        while (i <= j) {
            while (arr[i] < pivot1 || (arr[i] == pivot1 && arr2[i] < pivot2))
                i++;
            while (arr[j] > pivot1 || (arr[j] == pivot1 && arr2[j] > pivot2))
                j--;
            if (i <= j) {
                tmp = arr[i];
                tmp2 = arr2[i];

                /* Swap */
                arr[i] = arr[j];
                arr[j] = tmp;
                arr2[i] = arr2[j];
                arr2[j] = tmp2;

                i++;
                j--;
            }
        }

        return i;
    }


    private static int partition(long arr[],  byte[] values, int sizeOf, int left, int right)
    {
        int i = left, j = right;
        long tmp;
        int pivotidx = left + random.nextInt(right - left + 1);
        long pivot1 = arr[pivotidx];
        byte[] valueTemplate = new byte[sizeOf];

        while (i <= j) {
            while (arr[i] < pivot1)
                i++;
            while (arr[j] > pivot1)
                j--;
            if (i <= j) {
                tmp = arr[i];

                /* Swap */
                System.arraycopy(values, j * sizeOf, valueTemplate, 0, sizeOf);
                System.arraycopy(values, i * sizeOf, values, j * sizeOf, sizeOf);
                System.arraycopy(valueTemplate, 0, values, i * sizeOf, sizeOf);

                arr[i] = arr[j];
                arr[j] = tmp;

                i++;
                j--;
            }
        }

        return i;
    }


    public static int[] range(int until) {
        int[] a = new int[until];
        for(int i=0; i<until; i++) a[i] = i;
        return a;
    }

    static {
        System.loadLibrary("graphchi_sorting");
    }

    public static native void quickSort(long arr[], int arr2[]);

    public static native int[] radixSortWithIndex(long arr[]);

    public static void quickSortJava(long arr[], int arr2[], int left, int right) {
        if (left < right) {
            int index = partition(arr, arr2,  left, right);
            if (left < index - 1)
                quickSortJava(arr, arr2,  left, index - 1);
            if (index < right)
                quickSortJava(arr, arr2, index, right);
        }
    }

    public static int[] quickSortWithIndexJava(long[] arr) {
        int[] arr2 =range(arr.length);
        quickSortJava(arr, arr2, 0, arr.length - 1);
        return arr2;
    }







    public static void quickSort(long arr[], long arr2[],  byte[] values, int sizeOf, int left, int right) {
        if (left < right) {
            int index = partition(arr, arr2, values, sizeOf, left, right);
            if (left < index - 1)
                quickSort(arr, arr2, values, sizeOf, left, index - 1);
            if (index < right)
                quickSort(arr, arr2, values, sizeOf, index, right);
        }
    }
    static void quickSort(long arr[],  byte[] values, int sizeOf, int left, int right) {
        if (left < right) {
            int index = partition(arr, values, sizeOf, left, right);
            if (left < index - 1)
                quickSort(arr, values, sizeOf, left, index - 1);
            if (index < right)
                quickSort(arr,values, sizeOf, index, right);
        }
    }




    public static void sortWithValues(long[] shoveled, long[] shoveled2, byte[] edgeValues, int sizeOf) {
        quickSort(shoveled, shoveled2, edgeValues, sizeOf, 0, shoveled.length - 1);
    }


    public static void sortWithValues(long[] shoveled, byte[] edgeValues, int sizeOf) {
        quickSort(shoveled, edgeValues, sizeOf, 0, shoveled.length - 1);
    }

    /** Merges two doubly sorted arrays (sorted first by a, then by b) **/
    public static void mergeWithValues(long[] s1a, long[] s1b, byte[] values1, long[] s2a, long[] s2b, byte[] values2,
                                       long[] mergeda, long[] mergedb, byte[] mergedvalues, int sizeOf) {
        int idx1 = 0;
        int idx2 = 0;

        long x1a = (s1a.length > 0 ?  s1a[0] : Long.MAX_VALUE);
        long x2a = (s2a.length > 0 ? s2a[0] : Long.MAX_VALUE);

        long x1b = (s1a.length > 0 ?  s1b[0] : Long.MAX_VALUE);
        long x2b = (s2a.length > 0 ? s2b[0] : Long.MAX_VALUE);

        if (mergeda.length != s1a.length + s2a.length) throw new IllegalArgumentException("Arrays passed to merge wrong length");

        int l1last = s1a.length - 1;
        int l2last = s2a.length - 1;


        for(int mergeidx=0; mergeidx < mergeda.length; mergeidx++) {

            if (x1a < x2a || (x1a == x2a && x1b <= x2b)) {
                mergeda[mergeidx] = s1a[idx1];
                mergedb[mergeidx] = s1b[idx1];
                /* Data */
                System.arraycopy(values1,  idx1 * sizeOf, mergedvalues, mergeidx * sizeOf, sizeOf);
                if (idx1 < l1last) {
                    x1a = s1a[++idx1];
                    x1b = s1b[idx1];
                }
                else {
                    x1a = Long.MAX_VALUE;
                    x1b = Long.MAX_VALUE;
                }
            } else {
                mergeda[mergeidx] = s2a[idx2];
                mergedb[mergeidx] = s2b[idx2];
                /* Data */
                System.arraycopy(values2,  idx2 * sizeOf, mergedvalues, mergeidx * sizeOf, sizeOf);

                if (idx2 < l2last) {
                    x2a = s2a[++idx2];
                    x2b = s2b[idx2];
                }
                else {
                    x2a = Long.MAX_VALUE;
                    x2b = Long.MAX_VALUE;
                }

            }
        }
    }

    public static void testDoubleSorted(long[] s1a, long[] s1b) {
        for(int j=1; j<s1a.length; j++) {
            boolean ok = (s1a[j] > s1a[j-1] || (s1a[j] == s1a[j-1] && s1b[j] >= s1b[j-1]));
            if (!ok) throw new IllegalStateException("Array not sorted j =" + j + " (len=" + s1a.length + ")");
        }

    }

}
