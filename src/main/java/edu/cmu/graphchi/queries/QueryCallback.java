package edu.cmu.graphchi.queries;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Aapo Kyrola
 */
public interface QueryCallback {

    void receiveOutNeighbors(long vertexId, ArrayList<Long> neighborIds);

    void receiveInNeighbors(long vertexId, ArrayList<Long> neighborIds);

}
