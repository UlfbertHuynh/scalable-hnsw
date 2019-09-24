package ai.preferred.cerebro.hnsw;

import org.apache.lucene.util.PriorityQueue;

import java.util.ArrayList;
import java.util.function.Supplier;

public class RestrictedMaxHeap extends PriorityQueue<Candidate> {
    public RestrictedMaxHeap(int maxSize, Supplier<Candidate> sentinelObjectSupplier) {
        super(maxSize, sentinelObjectSupplier);
    }

    @Override
    protected boolean lessThan(Candidate a, Candidate b) {
        return a.distance > b.distance;
    }

    public int[] getCandidateIds(){
        Object [] arr = getHeapArray();
        int[] ids = new int[size()];
        for (int i = 1; i < size(); i++) {
            ids[i] = ((Candidate)arr[i]).nodeId;
        }
        return ids;
    }
    public ArrayList<Candidate> getListOfNonNull(){
        Object[] raw = getHeapArray();
        ArrayList<Candidate> res = new ArrayList<Candidate>(size());
        for (int i = 0; i < size(); i++) {
            res.add((Candidate) raw[i + 1]);
        }
        return res;
    }

}
