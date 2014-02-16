package edu.cmu.graphchi.bits;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Aapo Kyrola
 */
public class BitInputStream {

    private int bitOffset;
    private byte currentByte;
    private InputStream is;
    private long maxBits;
    private long bitsRead;

    public BitInputStream(InputStream is, long maxBits) throws IOException {
        this.is = is;
        this.maxBits = maxBits;

        if (maxBits > 0) currentByte = (byte) is.read();
        bitOffset = 0;
    }

    public boolean readBit() throws IOException {
        bitOffset ++;
        bitsRead ++;

        byte mask = (byte) (1 << (8-bitOffset));
        boolean bit = (currentByte & mask) != 0;

        if (bitOffset == 8 ) {
            bitOffset = 0;
            currentByte = (byte) is.read();
        }

        return bit;
    }


    public void seek(int bitIdx) {
        bitOffset = bitIdx;

    }

    public long getMaxBits() {
        return maxBits;
    }

    public void setMaxBits(long maxBits) {
        this.maxBits = maxBits;
    }

    public long getBitsRead() {
        return bitsRead;
    }

    public void setBitsRead(long bitsRead) {
        this.bitsRead = bitsRead;
    }

    public boolean eof() {
        return bitsRead >= maxBits;
    }
}
