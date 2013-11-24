
#include "edu_cmu_graphchi_util_Sorting.h"


#define MIN(a,b) ((a) < (b) ? (a) : (b))

int partition(jlong * arr, jint * arr2,  int left, int right)
{
    int i = left, j = right;
    long tmp;
    int tmp2;
    int len = right - left + 1;
    int pivotidx1  = left + len / 4;
    int pivotidx2 = MIN(left + 2 * len / 4, right - 1);
    int pivotidx3 = MIN(left + 3 * len / 4, right - 1);
    
    // Take median
    jlong p1 = arr[pivotidx1];
    jlong p2 = arr[pivotidx2];
    jlong p3 = arr[pivotidx3];
    
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
    
    
    jlong pivot1 = arr[pivotidx];
    jint pivot2 = arr2[pivotidx];
    
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

void quickSort(jlong * arr, jint * arr2, int left, int right) {
    if (left < right) {
        int index = partition(arr, arr2,  left, right);
        if (left < index - 1)
            quickSort(arr, arr2,  left, index - 1);
        if (index < right)
            quickSort(arr, arr2, index, right);
    }
}


JNIEXPORT void JNICALL Java_edu_cmu_graphchi_util_Sorting_quickSort
  (JNIEnv * env, jclass cl, jlongArray arr_, jintArray arr2_) {
      jboolean is_copy1;
      jboolean is_copy2;
      int n = env->GetArrayLength(arr_);
      jlong * arr = env->GetLongArrayElements(arr_, &is_copy1);
      jint * arr2 = env->GetIntArrayElements(arr2_, &is_copy2);
      
      quickSort(arr, arr2, 0, n - 1);
  }