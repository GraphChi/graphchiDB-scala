package edu.cmu.graphchi.queries;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Aapo Kyrola
 */
public interface QueryCallback {

    void receiveOutNeighbors(long vertexId, ArrayList<Long> neighborIds, ArrayList<Byte> edgeTypes, ArrayList<Long> dataPointers);

    void receiveInNeighbors(long vertexId, ArrayList<Long> neighborIds, ArrayList<Byte> edgeTypes, ArrayList<Long> dataPointers);

}
