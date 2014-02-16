package edu.cmu.graphchi.bits;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Aapo Kyrola
 */
public class BitOutputStream {


    private OutputStream out;

    private byte curByte = 0;
    private int curIdx;
    private long bitsWritten = 0;

    public BitOutputStream(OutputStream os) {
        this.out = os;
        curIdx = 0;
    }


    public void writeBits(boolean... bits) throws IOException {
        for(boolean b:bits) {
            writeBit(b);
        }
    }

    public void writeBit(boolean b) throws IOException {
        bitsWritten++;
        curIdx++;
        byte mask = (byte)(1 << (8-curIdx));
        if (b) curByte = (byte) (curByte | mask);
        if (curIdx == 8) {
            curIdx = 0;
            out.write(curByte);
            curByte = 0;
        }
    }




    public void close() throws IOException {
        if (curIdx > 0 ) out.write(curByte);
        out.flush();
    }

    public long getBitsWritten() {
        return bitsWritten;
    }

}
