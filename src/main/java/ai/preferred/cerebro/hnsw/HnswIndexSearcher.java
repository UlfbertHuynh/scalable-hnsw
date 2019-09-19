package ai.preferred.cerebro.hnsw;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ThreadInterruptedException;

import javax.management.Query;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class HnswIndexSearcher extends ParentHnsw {
    ExecutorService executor;
    public HnswIndexSearcher(String idxDir){
        super(idxDir);
        executor = Executors.newFixedThreadPool(nleaves);
        int maxNodeCount = 0;
        leaves = new LeafHnswSearcher[nleaves];
        //load all leaves
        for (int i = 0; i < nleaves; i++) {
            leaves[i] = new LeafHnswSearcher(this, i, idxDir);
            if (leaves[i].getNodeCount() > maxNodeCount)
                maxNodeCount = leaves[i].getNodeCount();
        }
        int finalMaxNodeCount = maxNodeCount;
        this.visitedBitSetPool = new GenericObjectPool<>(() -> new BitSet(finalMaxNodeCount), nleaves);
    }

    public TopDocs search(double[] query, int k){
        final int limit = Math.max(1, configuration.maxItemLeaf);
        final int cappedNumHits = Math.min(k, limit);

        final List<Future<TopDocs>> topDocsFutures = new ArrayList<>(nleaves);
        for (int i = 0; i < nleaves; ++i) {
            LeafHnswSearcher leaf = (LeafHnswSearcher) leaves[i];
            topDocsFutures.add(executor.submit(new Callable<TopDocs>() {
                @Override
                public TopDocs call() throws Exception {
                    return leaf.findNearest(query, k);
                }
            }));
        }
        int i =0;
        final TopDocs[] collectedTopdocs = new TopDocs[nleaves];
        for (Future<TopDocs> future : topDocsFutures) {
            try {
                collectedTopdocs[i] = future.get();
                i++;
            } catch (InterruptedException e) {
                throw new ThreadInterruptedException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return TopDocs.merge(0, cappedNumHits, collectedTopdocs, true);
    }
}
