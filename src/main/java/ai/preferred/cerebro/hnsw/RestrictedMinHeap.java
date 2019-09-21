package ai.preferred.cerebro.hnsw;

import org.apache.lucene.util.PriorityQueue;

import java.util.ArrayList;
import java.util.function.Supplier;

public class RestrictedMinHeap extends PriorityQueue<Candidate> {
    public RestrictedMinHeap(int maxSize, Supplier<Candidate> sentinelObjectSupplier) {
        super(maxSize, sentinelObjectSupplier);
    }

    @Override
    protected boolean lessThan(Candidate a, Candidate b) {
        return a.distance < b.distance;
    }


}
