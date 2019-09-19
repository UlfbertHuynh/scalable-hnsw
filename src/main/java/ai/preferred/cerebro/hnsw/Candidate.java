package ai.preferred.cerebro.hnsw;

import java.util.Comparator;

public class Candidate implements Comparable<Candidate>{
    final int nodeId;
    final double distance;
    final Comparator<Double> distanceComparator;

    Candidate(int nodeId, double distance, Comparator<Double> distanceComparator) {
        this.nodeId = nodeId;
        this.distance = distance;
        this.distanceComparator = distanceComparator;
    }

    @Override
    public int compareTo(Candidate o) {
        return distanceComparator.compare(distance, o.distance);
    }
}
