import org.apache.lucene.util.PriorityQueue;

import java.util.function.Supplier;

public class heap extends PriorityQueue<Integer> {
    public heap(int maxSize, Supplier<Integer> sentinelObjectSupplier) {
        super(maxSize, sentinelObjectSupplier);
    }

    @Override
    protected boolean lessThan(Integer a, Integer b) {
        return a < b;
    }
}
