
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
#include "edu_cmu_graphchi_util_Sorting.h"
#include <cstdlib>
#include <iostream>
#include <sys/time.h>
#include <assert.h>

#include "radixSort.h"
 
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

#define INSERTION_SORT_LIMIT 64

// Adapted from Pbbs's insertion sort by Guy Blelloch et al.
void insertionSort(jlong * A1, jint * A2, int n) {
    for (int i=0; i < n; i++) {
        jlong v = A1[i];
        jint v2 = A2[i];
        jlong* B1 = A1 + i - 1;
        jint* B2 = A2 + i - 1;
        while (B1 >= A1 && (v < *B1 || (v == *B1 && v2 < *B2))) {
           *(B1+1) = *B1;
           *(B2+1) = *B2;
            B1--; B2--;
        }
        *(B1+1) = v;
        *(B2+1) = v2;
    }
}

void quickSort(jlong * arr, jint * arr2, int left, int right) {
    
    if (left < right) {
        if (right - left  <= INSERTION_SORT_LIMIT) {
            insertionSort(arr + left, arr2 + left, (right - left) + 1);
        } else {
        
            int index = partition(arr, arr2,  left, right);
            if (left < index - 1)
                quickSort(arr, arr2,  left, index - 1);
            if (index < right)
                quickSort(arr, arr2, index, right);
            }
    }
}

struct long_with_index {
    jlong val;
    jint idx;
    long_with_index() {}
    long_with_index(jlong val, jint idx) : val(val), idx(idx) {}
    
   
};

inline bool operator< (long_with_index &a, long_with_index &b)
{
    return (a.val < b.val) || (a.val == b.val && a.idx < b.idx);
}

inline bool operator> (long_with_index &a, long_with_index &b)
{
    return (a.val > b.val) || (a.val == b.val && a.idx > b.idx);
}

void insertionSort(long_with_index * A1, int n) {
    for (int i=0; i < n; i++) {
        long_with_index v = A1[i];
        long_with_index* B1 = A1 + i - 1;
         while (B1 >= A1 && (v < *B1)) {
            *(B1+1) = *B1;
            B1--; 
        }
        *(B1+1) = v;
    }
}


int partition(long_with_index * arr, int left, int right)
{
    int i = left, j = right;
    long_with_index tmp;
    int len = right - left + 1;
    int pivotidx1  = left + len / 4;
    int pivotidx2 = MIN(left + 2 * len / 4, right - 1);
    int pivotidx3 = MIN(left + 3 * len / 4, right - 1);
    
    // Take median
    long_with_index p1 = arr[pivotidx1];
    long_with_index p2 = arr[pivotidx2];
    long_with_index p3 = arr[pivotidx3];
    
    int pivotidx = pivotidx1;
    if (p1 < p2 && p2 < p3) {
        pivotidx = pivotidx2;
    } else if (p3 < p2 && p2 < p1) {
        pivotidx = pivotidx2;
    } else if (p2 < p1 && p1 < p3) {
        pivotidx = pivotidx1;
    } else if (p3 < p1 && p1 < p2) {
        pivotidx = pivotidx1;
    } else pivotidx = pivotidx3;
    
    
    long_with_index pivot1 = arr[pivotidx];
    
    while (i <= j) {
        while (arr[i] < pivot1)
            i++;
        while (arr[j] > pivot1)
            j--;
        if (i <= j) {
            tmp = arr[i];
            
            /* Swap */
            arr[i] = arr[j];
            arr[j] = tmp;
            
            i++;
            j--;
        }
    }
    
    return i;
}



void quickSort(long_with_index * arr,  int left, int right) {
    
    if (left < right) {
        if (right - left  <= INSERTION_SORT_LIMIT) {
            insertionSort(arr + left, (right - left) + 1);
        } else {
            
            int index = partition(arr,left, right);
            if (left < index - 1)
                quickSort(arr, left, index - 1);
            if (index < right)
                quickSort(arr, index, right);
        }
    }
}

/** RADIX **/
 struct long_with_index_extract { 
     inline size_t operator() (jlong a) {return a;}
};


JNIEXPORT jintArray JNICALL Java_edu_cmu_graphchi_util_Sorting_radixSortWithIndex
(JNIEnv * env, jclass cl, jlongArray arr_) {
    jboolean is_copy1;
    int n = env->GetArrayLength(arr_);
    jlong * arr = env->GetLongArrayElements(arr_, &is_copy1);
    
     jlong maxid = 0;
    for(int i=0; i<n; i++) {
        if (arr[i] > maxid) maxid = arr[i];
        assert(arr[i] >= 0);
        arr[i] = arr[i] * n + i;
    }
     
    iSort(arr, (intT)n, intT(maxid)*n+n, long_with_index_extract());
    
    
    jintArray ret = env->NewIntArray(n);
    if (ret == NULL) {
        std::cerr << "JNI: could not create array of size " << n << std::endl;
        return ret;
    }
    jint * arr2 = new jint[n];
    for(int i=0; i<n; i++) {
        jlong tmp = arr[i];
        arr[i] = tmp / n;
        arr2[i] = tmp % n;
    }
 
    
    if (is_copy1) {
        env->SetLongArrayRegion(arr_, 0, n, arr);
    }
    env->ReleaseLongArrayElements(arr_, arr, 0);
        
    
    env->SetIntArrayRegion(ret, 0, n, arr2);
    delete [] arr2;
    return ret;
 }


JNIEXPORT void JNICALL Java_edu_cmu_graphchi_util_Sorting_quickSort
  (JNIEnv * env, jclass cl, jlongArray arr_, jintArray arr2_) {
      jboolean is_copy1;
      jboolean is_copy2;
      int n = env->GetArrayLength(arr_);
      jlong * arr = env->GetLongArrayElements(arr_, &is_copy1);
      jint * arr2 = env->GetIntArrayElements(arr2_, &is_copy2);
      
      quickSort(arr, arr2, 0, n - 1);
      
      if (is_copy1) {
          env->SetLongArrayRegion(arr_, 0, n, arr);
      }
      if (is_copy2) {
          env->SetIntArrayRegion(arr2_, 0, n, arr2);
      }
      env->ReleaseLongArrayElements(arr_, arr, 0);
      env->ReleaseIntArrayElements(arr2_, arr2, 0);
      
  }