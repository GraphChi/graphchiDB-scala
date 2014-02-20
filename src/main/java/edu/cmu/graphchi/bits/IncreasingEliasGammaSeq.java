package edu.cmu.graphchi.bits;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Stores an increasing sequence efficiently
 * @author Aapo Kyrola
 */
public class IncreasingEliasGammaSeq {

    private int length;
    private byte[] bits;

    private int[] indexBitIdx;
    private long[] indexValues;
    private int indexInterval = 128;


    public IncreasingEliasGammaSeq(long[] original) {
        length = original.length;

        encode(original);
    }



    int log2floor(long x) {
        int l = -1;
        while (x > 0) {
            x /= 2;
            l++;
        }
        return l;
    }

    private void encode(long[] original) {
        try {
            long prev = -1;
            ByteArrayOutputStream bos = new ByteArrayOutputStream(100000);
            BitOutputStream bitStream = new BitOutputStream(bos);

            int lastIndexEntry = 0;
            int indexSize = 1 + original.length / indexInterval;
            int indexIdx = 0;

            indexBitIdx = new int[indexSize];
            indexValues = new long[indexSize];
            for(int i=0; i<original.length; i++) {
                long delta = original[i] - prev;
                if (delta <= 0) {
                    System.err.println("Illegal delta: " + delta + " at position " + i);
                }
                assert(delta > 0);

                int numZeros = log2floor(delta);
                for(int j=0; j<numZeros; j++) {
                    bitStream.writeBit(false);
                }
                for(int j=numZeros; j>=0; j--) {
                    boolean bit = ((1L << j) & delta) != 0;
                    bitStream.writeBit(bit);
                }

                if ((i == 0) || (i - lastIndexEntry >= indexInterval && indexIdx < indexSize)) {
                    indexValues[indexIdx] = original[i];
                    indexBitIdx[indexIdx] = (int)bitStream.getBitsWritten();
                    indexIdx++;
                    lastIndexEntry = i;
                }

                prev = original[i];

            }
            bitStream.close();

            bits =  bos.toByteArray();

            while (indexIdx < indexSize) {
                indexValues[indexIdx] = original[original.length - 1];
                indexBitIdx[indexIdx] = (int)bitStream.getBitsWritten();
                indexIdx++;
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

        System.out.println("Encoded " + sizeInBytes() + " bytes vs. " + original.length * 8);
        System.out.println("Index size: " + (indexValues.length * 8 + indexBitIdx.length * 4) + " bytes");
    }

    public long get(int idx) {
        int indexIdx = idx / indexInterval;
        int curidx = (idx / indexInterval) * indexInterval;

        int bitIdx = indexBitIdx[indexIdx];
        long cumulant = indexValues[indexIdx];

        int currentByteIdx = bitIdx / 8;
        int bitOffset = bitIdx % 8;
        byte currentByte = bits[currentByteIdx];

        while(curidx < idx) {
            /* Prefix */
            boolean bit = false;
            int zeros = (-1);
            do {
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    currentByte = bits[currentByteIdx];
                }
                zeros++;
            } while(!bit);

            /* Bits */
            int delta = (1 << zeros);
            while(zeros > 0) {
                zeros--;
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    currentByte = bits[currentByteIdx];
                }
                if (bit) {
                    delta = delta | (1 << zeros);
                }
            }

            cumulant += delta;
            curidx++;
        }

        return cumulant;
    }

    // TODO: potential bug: there can be extra zero-bits in the end
    public int getIndex(long value) {
        int indexIdx = Arrays.binarySearch(indexValues, value);
        if (indexIdx < 0) {
            indexIdx = -(indexIdx + 1) - 1;
        }

        if (indexIdx < 0) {
            // Everything larger
            return -1;
        }

        int curidx = indexIdx * indexInterval;

        int bitIdx = indexBitIdx[indexIdx];
        long cumulant = indexValues[indexIdx];

        int currentByteIdx = bitIdx / 8;
        int bitOffset = bitIdx % 8;
        byte currentByte = bits[currentByteIdx];

        while(cumulant < value) {
            curidx++;

            /* Prefix */
            boolean bit = false;
            int zeros = (-1);
            do {
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    if (currentByteIdx == bits.length) return -1;
                    currentByte = bits[currentByteIdx];
                }
                zeros++;
            } while(!bit);

            /* Bits */
            int delta = (1 << zeros);
            while(zeros > 0) {
                zeros--;
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    if (currentByteIdx == bits.length) return -1;
                    currentByte = bits[currentByteIdx];
                }
                if (bit) {
                    delta = delta | (1 << zeros);
                }
            }

            cumulant += delta;
        }

        if (cumulant > value) return -1;

        return curidx;
    }

    public int getIndexOfLowerBound(long value) {
        int indexIdx = Arrays.binarySearch(indexValues, value);
        if (indexIdx < 0) {
            indexIdx = -(indexIdx + 1) - 1;
        }

        int curidx = indexIdx * indexInterval;

        int bitIdx = indexBitIdx[indexIdx];
        long cumulant = indexValues[indexIdx];

        int currentByteIdx = bitIdx / 8;
        int bitOffset = bitIdx % 8;
        byte currentByte = bits[currentByteIdx];

        while(cumulant < value) {
            curidx++;

            /* Prefix */
            boolean bit = false;
            int zeros = (-1);
            do {
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    if (currentByteIdx == bits.length) return -1;
                    currentByte = bits[currentByteIdx];
                }
                zeros++;
            } while(!bit);

            /* Bits */
            int delta = (1 << zeros);
            while(zeros > 0) {
                zeros--;
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    if (currentByteIdx == bits.length) return -1;
                    currentByte = bits[currentByteIdx];
                }
                if (bit) {
                    delta = delta | (1 << zeros);
                }
            }

            cumulant += delta;
        }

        if (cumulant == value) return curidx;
        return curidx - 1;
    }

    public long[] getTwo(int idx) {
        long[] ret = new long[2];
        getTwo(idx, ret);
        return ret;
    }

    /* @returns value at idx and idx + 1 */
    public void getTwo(int idx, long[] ret) {
        // Ugly code duplication, FIXME TODO
        int indexIdx = idx / indexInterval;
        int curidx = (idx / indexInterval) * indexInterval;

        int bitIdx = indexBitIdx[indexIdx];
        long cumulant = indexValues[indexIdx];

        int currentByteIdx = bitIdx / 8;
        int bitOffset = bitIdx % 8;
        byte currentByte = bits[currentByteIdx];

        while(curidx < idx + 1) {
            if (curidx == idx) {
                ret[0] = cumulant;
            }

            /* Prefix */
            boolean bit = false;
            int zeros = (-1);
            do {
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    currentByte = bits[currentByteIdx];
                }
                zeros++;
            } while(!bit);

            /* Bits */
            int delta = (1 << zeros);
            while(zeros > 0) {
                zeros--;
                bitOffset ++;
                byte mask = (byte) (1 << (8-bitOffset));
                bit = (currentByte & mask) != 0;
                if (bitOffset == 8 ) {
                    bitOffset = 0;
                    currentByteIdx++;
                    if (currentByteIdx <= bits.length - 1) {
                        currentByte = bits[currentByteIdx];
                    }
                }
                if (bit) {
                    delta = delta | (1 << zeros);
                }
            }

            cumulant += delta;
            curidx++;

        }
        ret[1] = cumulant;
    }

    /* Returns indices for the queryIds (which must be sorted) */
    public Iterator<Integer> iterator(final Iterator<Long> queryValuesIter) {
        return new Iterator<Integer>() {
            int j = -1;
            long cumulant = (-1);
            int bitOffset = 0;
            byte currentByte = (bits.length > 0 ? bits[0] : 0);
            int currentByteIdx = 0;

            @Override
            public boolean hasNext() {
                return queryValuesIter.hasNext();
            }

            @Override
            public Integer next() {
                long queryValue = queryValuesIter.next();

                 /* Check if cumulant far away, then jump some */
                if (queryValue - cumulant > 2048) {

                    int indexIdx = Arrays.binarySearch(indexValues, queryValue);
                    if (indexIdx < 0) {
                        indexIdx = -(indexIdx + 1) - 1;
                    }

                    int newIdx = indexIdx * indexInterval;
                    if (newIdx > j) {  // jump only forward
                        int bitIdx = indexBitIdx[indexIdx];
                        cumulant = indexValues[indexIdx];

                        if (cumulant > queryValue) {
                            throw new IllegalStateException();
                        }

                        currentByteIdx = bitIdx / 8;
                        bitOffset = bitIdx % 8;
                        currentByte = bits[currentByteIdx];
                        j = newIdx;
                    }
                }


                 /* Prefix */
                while(cumulant < queryValue) {
                    j++;

                    boolean bit = false;
                    int zeros = (-1);
                    do {
                        bitOffset ++;
                        byte mask = (byte) (1 << (8-bitOffset));
                        bit = (currentByte & mask) != 0;
                        if (bitOffset == 8 ) {
                            bitOffset = 0;
                            currentByteIdx++;
                            currentByte = bits[currentByteIdx];
                        }
                        zeros++;
                    } while(!bit);

                /* Bits */
                    int delta = (1 << zeros);
                    while(zeros > 0) {
                        zeros--;
                        bitOffset ++;
                        byte mask = (byte) (1 << (8-bitOffset));
                        bit = (currentByte & mask) != 0;
                        if (bitOffset == 8 ) {
                            bitOffset = 0;
                            currentByteIdx++;
                            if (currentByteIdx < bits.length - 1) {
                                currentByte = bits[currentByteIdx];
                            }
                        }
                        if (bit) {
                            delta = delta | (1 << zeros);
                        }
                    }

                    cumulant += delta;
                }
                if (cumulant > queryValue) return -1; // Not found
                return j;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Iterator<Long> iterator() {
        return new Iterator<Long>() {
            int j = 0;
            long cumulant = (-1);
            int bitOffset = 0;
            byte currentByte = (bits.length > 0 ? bits[0] : 0);
            int currentByteIdx = 0;

            @Override
            public boolean hasNext() {
                return j < length - 1;
            }

            @Override
            public Long next() {
                 /* Prefix */
                boolean bit = false;
                int zeros = (-1);
                do {
                    bitOffset ++;
                    byte mask = (byte) (1 << (8-bitOffset));
                    bit = (currentByte & mask) != 0;
                    if (bitOffset == 8 ) {
                        bitOffset = 0;
                        currentByteIdx++;
                        currentByte = bits[currentByteIdx];
                    }
                    zeros++;
                } while(!bit);

            /* Bits */
                int delta = (1 << zeros);
                while(zeros > 0) {
                    zeros--;
                    bitOffset ++;
                    byte mask = (byte) (1 << (8-bitOffset));
                    bit = (currentByte & mask) != 0;
                    if (bitOffset == 8 ) {
                        bitOffset = 0;
                        currentByteIdx++;
                        if (currentByteIdx < bits.length - 1) {
                            currentByte = bits[currentByteIdx];
                        }
                    }
                    if (bit) {
                        delta = delta | (1 << zeros);
                    }
                }

                cumulant += delta;
                j++;
                return cumulant;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public int length() {
        return length;
    }

    public int sizeInBytes() {
        return bits.length + (indexValues.length * 8 + indexBitIdx.length * 4);
    }


}
