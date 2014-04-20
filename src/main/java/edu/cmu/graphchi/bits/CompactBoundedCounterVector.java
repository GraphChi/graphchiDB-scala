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

import edu.cmu.graphchidb.storage.ByteConverter;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Used to represent N counters, each with a small bounded value. For example,
 * if bounded value is 7, we use 3 bits for each counter.
 * One example use is to store state of multiple parallel BFSes for each vertex.
 * @author Aapo Kyrola
 */
public class CompactBoundedCounterVector {

    int n;
    int bitsPerCounter;
    byte[] bytes;


    public CompactBoundedCounterVector(int n, int bitsPerCounter) {
        this.n = n;
        this.bitsPerCounter = bitsPerCounter;
        this.bytes = new byte[n * bitsPerCounter / 8 + ((n * bitsPerCounter) % 8 == 0 ? 0 : 1)];
        if (bitsPerCounter > 8) {
            throw new IllegalArgumentException("Max 8 bits per counter!");
        }
    }

    public CompactBoundedCounterVector(int[] arr, int bitsPerCounter) {
        this(arr.length, bitsPerCounter);
        int mx = getMaxCount();
        for(int j=0; j<arr.length; j++) set(j, (byte) (arr[j] < mx ? arr[j] : mx));
    }

    public int size() {
        return n;
    }

    public int getMaxCount() {
        return (1<<bitsPerCounter) - 1;
    }

    public void increment(int index) {
        int val = get(index);
        if (val < getMaxCount()) {
            val++;
            set(index, (byte)val);
        }
    }

    public void incrementAll() {
        for(int i=0; i < n; i++) increment(i);
    }

    public void incrementAllNonZero() {
        for(int i=0; i < n; i++) {
            if (get(i) > 0) increment(i);
        }
    }

    public int[] toIntArray() {
        int[] arr = new int[size()];
        for(int j=0; j<arr.length; j++) {
            arr[j] = get(j);
        }
        return arr;
    }

    public void set(int index, byte val) {
        int bitIndex = index * bitsPerCounter;
        for(int j=0; j<bitsPerCounter; j++) {
            int byteIndex = bitIndex / 8;
            int bitOffset = bitIndex % 8;

            byte currentByte = bytes[byteIndex];
            byte writeMask = (byte) (1 << bitOffset);
            byte writeNotMask = (byte) ~writeMask;
            byte readBit = (byte) (((val &  (byte) (1 << j)) >> j) << bitOffset);

            bytes[byteIndex] = (byte) ((currentByte & writeNotMask) | readBit);
            bitIndex++;
        }
    }

    public int get(int index) {
        int bitIndex = index * bitsPerCounter;
        int a = 0;
        for(int j=0; j<bitsPerCounter; j++) {
            int byteIndex = bitIndex / 8;
            int bitOffset = bitIndex % 8;

            byte currentByte = bytes[byteIndex];
            byte mask = (byte) (1 << bitOffset);
            boolean bit = (currentByte & mask) != 0;
            if (bit) {
                a |= (1 << j);
            }
            bitIndex++;
        }
        return a;
    }

    public ByteConverter<CompactBoundedCounterVector> getByteConverter() {
        final int len = bytes.length;
        final int _n = n;
        final int _bitsPerCounter = bitsPerCounter;
        return new ByteConverter<CompactBoundedCounterVector>() {
            @Override
            public CompactBoundedCounterVector fromBytes(ByteBuffer bb) {
                CompactBoundedCounterVector cv = new CompactBoundedCounterVector(_n, _bitsPerCounter);
                bb.get(cv.bytes);
                return cv;
            }

            @Override
            public void toBytes(CompactBoundedCounterVector v, ByteBuffer out) {
                out.put(v.bytes);
            }

            @Override
            public int sizeOf() {
                return len;
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompactBoundedCounterVector that = (CompactBoundedCounterVector) o;

        if (bitsPerCounter != that.bitsPerCounter) return false;
        if (n != that.n) return false;
        if (!Arrays.equals(bytes, that.bytes)) return false;

        return true;
    }

    public String toString() {
        String s = "";
        for(int i=0; i<size(); i++) s += get(i) + ", ";
        return s;
    }

    // Helper method used by MultiBFS
    public static void pointwiseMinOfNonzeroesIncrementByOne(int[] arr1, CompactBoundedCounterVector counter2) {
        int n = arr1.length;
        for(int j=0; j<n; j++) {
            int b = counter2.get(j);
            if (b != 0) {
                if (arr1[j] == 0) arr1[j] = b + 1;
                else arr1[j] = Math.min(arr1[j], b + 1);
            }
        }
    }
}
