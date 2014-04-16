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
package edu.cmu.graphchi.util;

import org.junit.Test;

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
        testMap.compute();

        for(int i=1000000; i<1000300; i++) {
            assertEquals(i, testMap.getId("id" + i));
        }

        assertEquals(-1, testMap.getId("huuhaa"));

    }
}
