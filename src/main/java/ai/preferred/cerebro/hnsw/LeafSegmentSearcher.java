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

        PriorityQueue<Candidate> topCandidates = searchLayer(currObj, query, Math.max(ef, k), 0);

        while (topCandidates.size() > k) {
            topCandidates.poll();
        }
        ScoreDoc[] hits = new ScoreDoc[topCandidates.size()];
        for (int i = topCandidates.size() - 1; i >= 0; i--) {
            Candidate h = topCandidates.poll();
            hits[i] = new ScoreDoc(nodes[h.nodeId].item.externalId, (float) (1 - h.distance));
        }


        return new TopDocs(topCandidates.size(), hits, hits[0].score);
    }

    @Override
    protected PriorityQueue<Candidate> searchLayer(
            Node entryPointNode, double[] query, int k, int layer) {

        BitSet visitedBitSet = parent.getBitsetFromPool();

        try {
            PriorityQueue<Candidate> topCandidates =
                    new PriorityQueue<>(Comparator.<Candidate>naturalOrder().reversed());
            PriorityQueue<Candidate> checkNeighborSet = new PriorityQueue<>();

            double distance = distanceFunction.distance(query, entryPointNode.vector());

            Candidate firstCandidade = new Candidate(entryPointNode.internalId, distance, distanceComparator);

            topCandidates.add(firstCandidade);
            checkNeighborSet.add(firstCandidade);
            visitedBitSet.flipTrue(entryPointNode.internalId);

            double lowerBound = distance;

            while (!checkNeighborSet.isEmpty()) {

                Candidate curCandidate = checkNeighborSet.poll();

                if (greater(curCandidate.distance, lowerBound)) {
                    break;
                }

                Node node = nodes[curCandidate.nodeId];

                MutableIntList candidates = node.outConns[layer];

                for (int i = 0; i < candidates.size(); i++) {

                    int candidateId = candidates.get(i);

                    if (!visitedBitSet.isTrue(candidateId)) {

                        visitedBitSet.flipTrue(candidateId);

                        double candidateDistance = distanceFunction.distance(query,
                                nodes[candidateId].vector());

                        if (greater(topCandidates.peek().distance, candidateDistance) || topCandidates.size() < k) {

                            Candidate newCandidate = new Candidate(candidateId, candidateDistance, distanceComparator);

                            checkNeighborSet.add(newCandidate);
                            topCandidates.add(newCandidate);

                            if (topCandidates.size() > k) {
                                topCandidates.poll();
                            }

                            lowerBound = topCandidates.peek().distance;
                        }
                    }
                }
            }
            return topCandidates;
        } finally {
            visitedBitSet.clear();
            parent.returnBitsetToPool(visitedBitSet);
        }
    }
}
