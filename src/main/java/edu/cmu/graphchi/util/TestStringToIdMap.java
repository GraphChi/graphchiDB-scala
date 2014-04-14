package edu.cmu.graphchi.util;

import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Aapo Kyrola
 */
public class TestStringToIdMap {

    @Test
    public void testStringToIdMapBasics() {
        StringToIdMap testMap = new StringToIdMap(128);

        for(int i=1000000; i<1000300; i++) {
            testMap.put("id" + i, i);
        }

        for(int i=1000000; i<1000300; i++) {
            assertEquals(i, testMap.getId("id" + i));
        }

    }
}
