package edu.cmu.graphchi.bits;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Aapo Kyrola
 */
public class BitStreamTests {
    @Test
    public void testInteroperability() throws Exception {
        int N = 100003;
        boolean[] data = new boolean[N];
        Random r = new Random();
        for(int i=0; i<N; i++) data[i] = r.nextInt() % 2 == 1;

        ByteArrayOutputStream bos = new ByteArrayOutputStream(N/8+1);
        BitOutputStream bitOs  = new BitOutputStream(bos);
        bitOs.writeBits(data);
        bitOs.close();

        assertEquals(N, bitOs.getBitsWritten());


        BitInputStream bitIn = new BitInputStream(new ByteArrayInputStream(bos.toByteArray()), bitOs.getBitsWritten());


        long i = 0;
        while(!bitIn.eof()) {
            assertEquals(data[(int) i++], bitIn.readBit());
        }

        assertEquals(i, bitOs.getBitsWritten());
    }
}
