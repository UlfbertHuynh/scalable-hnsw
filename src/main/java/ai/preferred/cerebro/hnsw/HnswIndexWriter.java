package ai.preferred.cerebro.hnsw;


import ai.preferred.cerebro.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manager class for creating leaf indexes (leaf segments), inserting samples
 * and automatically handling the creation of leaf segments during runtime if
 * capacity of all existing leaves is reached or providing an interface to do
 * so manually.
 * @param <TVector> the type of vector supported.
 *
 * @author hpminh@apcs.vn
 */

public final class HnswIndexWriter<TVector> extends ParentHnsw<TVector>
        implements ConcurrentWriter<TVector>{

    private final int OPTIMAL_NUM_LEAVES;


    /**
     * Contructor for creating a new index at a directory, will throw error
     * if another hnsw index has already resided in that directory
     *
     * @param configuration object containing the configurable setting of a hnsw index
     * @param dir the directory to build the index
     */
    public HnswIndexWriter(HnswConfiguration configuration, String dir) {
        if (!isSafeToCreate(dir)){
            throw new IllegalArgumentException("An index has already resided in this directory. Can only modify.");
        }
        this.configuration = configuration;
        this.idxDir = dir;
        OPTIMAL_NUM_LEAVES = Runtime.getRuntime().availableProcessors();

        this.visitedBitSetPool = new GenericObjectPool<>(() -> new BitSet(configuration.maxItemLeaf), Math.max(OPTIMAL_NUM_LEAVES, nleaves));


        if (configuration.lowMemoryMode)
            nleaves = 1;
        else
            //Initialize all leaves with default max num of nodes
            nleaves = OPTIMAL_NUM_LEAVES;
        lookup = new ConcurrentHashMap<>(nleaves * configuration.maxItemLeaf);

        leaves = new LeafSegmentWriter[nleaves];
        int baseNewLeaf = 0;
        for (int i = 0; i < nleaves; i++) {
            if (configuration.lowMemoryMode)
                leaves[i] = new LeafSegmentBlockingWriter<>(this, i, baseNewLeaf);
            else
                leaves[i] = new LeafSegmentWriter<>(this, i, baseNewLeaf);
            baseNewLeaf += configuration.maxItemLeaf;
        }

    }

    /**
     * Contructor for loading up an already existing index at a directory for
     * modifying (insert more samples, delete samples, update samples)
     * @param dir the directory containing the index
     */
    public HnswIndexWriter(String dir){
        super(dir);
        OPTIMAL_NUM_LEAVES = Runtime.getRuntime().availableProcessors();
        this.visitedBitSetPool = new GenericObjectPool<>(() -> new BitSet(configuration.maxItemLeaf), nleaves);
        //load all leaves
        for (int i = 0; i < nleaves; i++) {
            leaves[i] = new LeafSegmentWriter<>(this, i, idxDir);
        }
    }

    private boolean isSafeToCreate(String idxDir){
        File file = new File(idxDir + globalConfigFileName);
        return !IndexUtils.checkFileExist(file);
    }


    private synchronized LeafSegmentWriter growNewLeaf(String leafInAction, boolean isLeafBlocking){
        if (leafInAction.compareTo(leaves[nleaves - 1].leafName) == 0) {
            System.out.println("Current segment reached maximum capacity, creating and switching to use a new segment.");
            if (leaves.length == nleaves){
                LeafSegment<TVector>[] hold = leaves;
                leaves = new LeafSegmentWriter[nleaves + 5];
                System.arraycopy(hold, 0, leaves, 0, hold.length);
            }
            if (isLeafBlocking)
                leaves[nleaves] = new LeafSegmentBlockingWriter<>(this, nleaves,configuration.maxItemLeaf * nleaves++);
            else
                leaves[nleaves] = new LeafSegmentWriter<>(this, nleaves,configuration.maxItemLeaf * nleaves++);
            return (LeafSegmentWriter) leaves[nleaves - 1];
        }
        else if(leafInAction.compareTo(leaves[nleaves - 1].leafName) < 0){
            return (LeafSegmentWriter) leaves[nleaves - 1];
        }
        else
            throw new IllegalArgumentException("In-action leaf's index should not be greater than the number of leaves minus one");
    }

    /**
     * removing a sample by its external ID
     * @param externalID external ID of the vector sample
     */
    public void removeOnExternalID(int externalID) {
        int globalID = lookup.get(externalID);
        int leafNum = globalID / configuration.maxItemLeaf;
        int internalID = globalID % configuration.maxItemLeaf;
        lookup.remove(externalID);
        ((LeafSegmentWriter)leaves[leafNum]).removeOnInternalID(internalID);
    }


    private String checkCapacity(int amountToInsert){
        int remainingSlots = 0;
        for (int i = 0; i < nleaves; i++) {
            remainingSlots += leaves[i].maxNodeCount - leaves[i].size();
        }
        if(nleaves < OPTIMAL_NUM_LEAVES){
            remainingSlots += (OPTIMAL_NUM_LEAVES - nleaves) * configuration.maxItemLeaf;
        }
        if (remainingSlots >= amountToInsert)
            return null;
        else
            return "Not enough space, call expand() before add. Operation failed." +
                    "\nSpace needed: " + amountToInsert + ", Space had: " + remainingSlots;
    }

    /**
     * return the number of samples across all the leaf segments of the index
     * @return
     */
    public int size() {
        int size = 0;
        for (int i = 0; i < nleaves; i++) {
            size += leaves[i].size();
        }
        return size;
    }
    /*
    public Optional<double[]> get(int luceneId) {
        synchronized (lookup){
            Integer globalID = lookup.get(luceneId);
            if(globalID == null)
                return Optional.empty();
            else{
                int idxleaf = globalID / configuration.maxItemLeaf;
                int leafId = globalID % configuration.maxItemLeaf;
                return leaves[idxleaf].getVec(leafId);
            }
        }
    }
     */

    /**
     * Insert the samples into the index
     * @param items the items to add to the index
     * @throws InterruptedException
     */
    @Override
    public void addAll(Collection<Item<TVector>> items) throws InterruptedException {
        String message = checkCapacity(items.size());
        if(message == null){
            if (configuration.lowMemoryMode)
                singleSegmentAddAll(items, OPTIMAL_NUM_LEAVES, CLIProgressListener.INSTANCE, DEFAULT_PROGRESS_UPDATE_INTERVAL);
            else
                addAll(items, nleaves, CLIProgressListener.INSTANCE, DEFAULT_PROGRESS_UPDATE_INTERVAL);
        }

        else
            throw new IllegalArgumentException(message);
    }

    @Override
    public void addAll(Collection<Item<TVector>> items, int numThreads, ProgressListener listener, int progressUpdateInterval) throws InterruptedException {
        AtomicReference<RuntimeException> throwableHolder = new AtomicReference<>();

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads, new NamedThreadFactory("indexer-%d"));

        AtomicInteger workDone = new AtomicInteger();

        try{
            Queue<Item> queue = new LinkedBlockingDeque<>(items);
            CountDownLatch latch = new CountDownLatch(numThreads);
            int maxsize = items.size();
            for (int threadId = 0; threadId < numThreads; threadId++)
                executorService.submit(
                        new InsertItemTask((LeafSegmentWriter)leaves[threadId],
                                queue,
                                maxsize,
                                throwableHolder,
                                workDone,
                                latch,
                                listener));

            latch.await();

            RuntimeException throwable = throwableHolder.get();
            if (throwable != null)
                throw throwable;
            }
        finally {
            executorService.shutdown();
        }
    }

    public void singleSegmentAddAll(Collection<Item<TVector>> items, int numThreads, ProgressListener listener, int progressUpdateInterval) throws InterruptedException {
        AtomicReference<RuntimeException> throwableHolder = new AtomicReference<>();

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads,
                new NamedThreadFactory("indexer-%d"));

        AtomicInteger workDone = new AtomicInteger();

        try {
            Queue<Item<TVector>> queue = new LinkedBlockingDeque<>(items);

            CountDownLatch latch = new CountDownLatch(numThreads);
            //final AtomicInteger idxleafInAction = new AtomicInteger(nleaves - 1);
            LeafSegmentBlockingWriter<TVector> leafInAction = (LeafSegmentBlockingWriter<TVector>) leaves[nleaves - 1];
            for (int threadId = 0; threadId < numThreads; threadId++) {

                executorService.submit(() -> {
                    LeafSegmentBlockingWriter<TVector> leaf = leafInAction;
                    Item<TVector> item;
                    while(throwableHolder.get() == null && (item = queue.poll()) != null) {
                        try {
                            boolean signal = leaf.add(item);
                            if (signal){
                                int done = workDone.incrementAndGet();

                                if (done % progressUpdateInterval == 0) {
                                    listener.updateProgress(done, items.size());
                                }
                            }
                            //here we assume that add(item) return false when
                            //the segment when reached its maximum capacity
                            else {
                                leaf = (LeafSegmentBlockingWriter<TVector>) growNewLeaf(leaf.leafName, true);
                                leaf.add(item);
                            }
                        } catch (RuntimeException t) {
                            throwableHolder.set(t);
                        }
                    }

                    latch.countDown();
                });
            }

            latch.await();

            RuntimeException throwable = throwableHolder.get();

            if (throwable != null) {
                throw throwable;
            }

        } finally {
            executorService.shutdown();
        }
    }

    /**
     * save the index into concrete files. Make sure to call this function before
     * terminating. Otherwise all information is lost.
     * @throws IOException
     */
    @Override
    public void save() throws IOException {
        synchronized (configuration){
            Kryo kryo = new Kryo();
            kryo.register(String.class);
            try {
                Output output = new Output(new FileOutputStream(idxDir + globalConfigFileName));
                kryo.writeObject(output, configuration.handler.getClass().getCanonicalName());
                kryo.writeObject(output, configuration.m);
                kryo.writeObject(output, configuration.ef);
                kryo.writeObject(output, configuration.efConstruction);
                kryo.writeObject(output, configuration.removeEnabled);
                kryo.writeObject(output, configuration.lowMemoryMode);
                kryo.writeObject(output, configuration.maxItemLeaf);
                kryo.writeObject(output, nleaves);
                output.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        synchronized (lookup){
            Kryo kryo = new Kryo();
            kryo.register(Integer.class);
            kryo.register(ConcurrentHashMap.class);
            try {
                Output output = new Output(new FileOutputStream(idxDir + globalLookupFileName));
                kryo.writeObject(output, lookup);
                output.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        for (int i = 0; i < nleaves; i++) {
            ((LeafSegmentWriter)leaves[i]).save(idxDir);
        }
    }

    static private class InsertItemTask implements Runnable{
        final static int DEFAULT_PROGRESS_UPDATE_INTERVAL = 1_000;
        final private Queue<Item> itemQueue;
        final private AtomicReference<RuntimeException> throwableHolder;
        final private AtomicInteger workDone;
        final private CountDownLatch latch;
        final private ProgressListener listener;
        private LeafSegmentWriter leaf;
        final private int max;
        InsertItemTask(LeafSegmentWriter leaf,
                       Queue<Item> itemQueue,
                       int maxsize,
                       AtomicReference<RuntimeException> throwableHolder,
                       AtomicInteger workDone,
                       CountDownLatch latch,
                       ProgressListener listener){
            this.leaf = leaf;
            this.itemQueue = itemQueue;
            this.throwableHolder = throwableHolder;
            this.workDone = workDone;
            this.latch = latch;
            this.listener = listener;
            max = maxsize;
        }
        @Override
        public void run() {
            Item item;
            while(throwableHolder.get() == null && (item = itemQueue.poll()) != null) {
                try {
                    if (leaf.add(item)){
                        int done = workDone.incrementAndGet();
                        if (done % DEFAULT_PROGRESS_UPDATE_INTERVAL == 0) {
                            listener.updateProgress(done, max);
                        }
                    }
                    else
                        itemQueue.add(item);

                } catch (RuntimeException t) {
                    throwableHolder.set(t);
                }
            }
            latch.countDown();
        }
    }
}
