package edu.cmu.graphchi.engine;

/**
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
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
 */

public class VertexInterval {

    private long firstVertex;
    private long lastVertex; // Inclusive

    public VertexInterval(long firstVertex, long lastVertex) {
        this.firstVertex = firstVertex;
        this.lastVertex = lastVertex;
    }



    public long getFirstVertex() {
        return firstVertex;
    }



    public long getLastVertex() {
        return lastVertex;
    }


    public String toString() {
        return "Interval " + firstVertex + " -- " + lastVertex;
    }

}
