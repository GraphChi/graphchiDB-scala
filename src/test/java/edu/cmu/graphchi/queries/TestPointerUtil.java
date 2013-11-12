package edu.cmu.graphchi.queries;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Aapo Kyrola
 */
public class TestPointerUtil {

    @Test
    public void testEncodingDecoding() {
        int shardNum = 49;
        int shardPos = 29193134;

        Random r = new Random(260379);
        for(int i=0; i<1000; i++) {
            long pointer = PointerUtil.encodePointer(shardNum, shardPos);
            assertEquals(shardNum, PointerUtil.decodeShardNum(pointer));
            assertEquals(shardPos, PointerUtil.decodeShardPos(pointer));

            shardNum = Math.abs(r.nextInt() % 1024);
            shardPos = Math.abs(r.nextInt());
        }
    }
}
