package edu.cmu.graphchi.bits;

import edu.cmu.graphchidb.storage.ByteConverter;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * @author Aapo Kyrola
 */
public class TestCompactBoundedCounterVector {

    @Test
    public void testSevenCounter() {
        CompactBoundedCounterVector counter = new CompactBoundedCounterVector(100, 3);

        assertEquals(7, counter.getMaxCount());
        assertEquals(100, counter.size());

        for(int i=0; i<100; i++) {
            assertEquals(0, counter.get(i));
        }

        counter.increment(4);
        assertEquals(1, counter.get(4));

        counter.set(4, (byte)3);
        assertEquals(3, counter.get(4));

        counter.set(4, (byte)0);
        assertEquals(0, counter.get(4));

        counter.set(4, (byte)7);
        assertEquals(7, counter.get(4));

        // Check that changing one value does not inadvertently change other values
        for(int i=0; i<100; i++) {
            if (i != 4) assertEquals(0, counter.get(i));
        }

        for(int j=0; j<1000; j++) {
            counter.increment(88);
            if (j + 1 < 8) {
                assertEquals(j + 1, counter.get(88));
            } else {
                assertEquals(7, counter.get(88));
            }
        }

        for(int i=0; i<100; i++) {
            if (i != 4 && i != 88) assertEquals(0, counter.get(i));
        }

        for(int j=0; j<10; j++) {
            for(int i=0; i<100; i++) {
                if (i != 4 && i != 88) {
                    counter.increment(i);
                    if (j + 1< 8) {
                        assertEquals(j + 1, counter.get(i));
                    } else {
                        assertEquals(7, counter.get(i));
                    }
                }
            }
        }
    }

    @Test
    public void testByteConverter() {
        int n = 55;
        int bits = 5;
        ByteBuffer bb = ByteBuffer.allocate(n * bits / 8 + 1);
        CompactBoundedCounterVector counter = new CompactBoundedCounterVector(n, bits);

        assertEquals((1 << 5) - 1, counter.getMaxCount());

        int mx = counter.getMaxCount();

        for(int j=0; j<n; j++) {
            counter.set(j,  (byte) (j % counter.getMaxCount()));
            assertEquals(j % mx, counter.get(j));
        }

        ByteConverter<CompactBoundedCounterVector> conv = counter.getByteConverter();
        conv.toBytes(counter, bb);

        bb.rewind();

        counter = null;
        CompactBoundedCounterVector newCounter = conv.fromBytes(bb);
        for(int j=0; j<n; j++) {
            assertEquals(j % mx, newCounter.get(j));
        }
    }
}
