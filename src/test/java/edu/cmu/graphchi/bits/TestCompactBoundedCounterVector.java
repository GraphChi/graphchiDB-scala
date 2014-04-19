package edu.cmu.graphchi.bits;

import org.junit.Test;

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
}
