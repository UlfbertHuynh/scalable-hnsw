package ai.preferred.cerebro.hnsw;

import org.apache.lucene.search.*;
import org.apache.lucene.util.ThreadInterruptedException;

import java.util.*;
import java.util.concurrent.*;


/**
 * Manager class to start search on all leaf segments and then aggregate
 * (using K-way merging algorithm) their results to get the top k.
 *
 * @param <TVector> type of the vector supported
 *
 * @author hpminh@apcs.vn
 */
public class HnswIndexSearcher<TVector> extends ParentHnsw<TVector> {
    ExecutorService executor;

    /**
     * Load into memory all the leaf segments of an already existing index
     * @param idxDir
     */
    public HnswIndexSearcher(String idxDir){
        super(idxDir);
        executor = Executors.newFixedThreadPool(nleaves);
        int maxNodeCount = 0;
        leaves = new LeafSegmentSearcher[nleaves];
        //load all leaves
        for (int i = 0; i < nleaves; i++) {
            leaves[i] = new LeafSegmentSearcher<>(this, i, idxDir);
            if (leaves[i].getNodeCount() > maxNodeCount)
                maxNodeCount = leaves[i].getNodeCount();
        }
        int finalMaxNodeCount = maxNodeCount;
        this.visitedBitSetPool = new GenericObjectPool<>(() -> new BitSet(finalMaxNodeCount), nleaves);
    }

    /**
     * conduct search on all leaf segment then aggregate
     * @param query the query vectors
     * @param k the number of top results to be selected
     * @return the internal Ids of the top results and their scores
     */
    public TopDocs search(TVector query, int k){
        final int limit = Math.max(1, configuration.maxItemLeaf);
        final int cappedNumHits = Math.min(k, limit);

        final List<Future<TopDocs>> topDocsFutures = new ArrayList<>(nleaves);
        for (int i = 0; i < nleaves; ++i) {
            LeafSegmentSearcher<TVector> leaf = (LeafSegmentSearcher<TVector>) leaves[i];
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
