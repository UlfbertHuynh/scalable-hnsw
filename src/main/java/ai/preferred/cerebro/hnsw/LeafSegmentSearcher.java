package ai.preferred.cerebro.hnsw;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.util.*;

public class LeafSegmentSearcher extends LeafSegment {

    LeafSegmentSearcher(ParentHnsw parent, int numName, String idxDir) {
        super(parent, numName, idxDir, Mode.SEARCH);
    }

    public TopDocs findNearest(double[] query, int k) {

        if (entryPoint == null) {
            return new TopDocs(0, null, Float.NaN);
        }

        Node entryPointCopy = entryPoint;

        Node currObj = entryPointCopy;

        double curDist = distanceFunction.distance(query, currObj.vector());

        for (int activeLevel = entryPointCopy.maxLevel(); activeLevel > 0; activeLevel--) {

            boolean changed = true;
            while (changed){
                changed = false;
                IntArrayList candidateConnections = currObj.outConns[activeLevel];

                for (int i = 0; i < candidateConnections.size(); i++) {

                    int candidateId = candidateConnections.get(i);

                    double candidateDistance = distanceFunction.distance(query, nodes[candidateId].vector());
                    if (lesser(candidateDistance, curDist)) {
                        curDist = candidateDistance;
                        currObj = nodes[candidateId];
                        changed = true;
                    }
                }
            }
        }

        RestrictedMaxHeap topCandidates = searchLayer(currObj, query, Math.max(ef, k), 0);

        while (topCandidates.size() > k) {
            topCandidates.pop();
        }
        ScoreDoc[] hits = new ScoreDoc[topCandidates.size()];
        for (int i = topCandidates.size() - 1; i >= 0; i--) {
            Candidate h = topCandidates.pop();
            hits[i] = new ScoreDoc(nodes[h.nodeId].item.externalId, (float) (1 - h.distance));
        }


        return new TopDocs(topCandidates.size(), hits, hits[0].score);
    }
}
