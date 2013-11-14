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



    static void quickSort(long arr[], long arr2[],  byte[] values, int sizeOf, int left, int right) {
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

}
