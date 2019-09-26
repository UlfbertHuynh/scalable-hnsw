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

public final class HnswIndexWriter extends ParentHnsw
        implements ConcurrentWriter{

    private final int OPTIMAL_NUM_LEAVES;


    //Create Constructor
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
                leaves[i] = new LeafSegmentBlockingWriter(this, i, baseNewLeaf);
            else
                leaves[i] = new LeafSegmentWriter(this, i, baseNewLeaf);
            baseNewLeaf += configuration.maxItemLeaf;
        }

    }

    //Load up an already created index for modifying
    public HnswIndexWriter(String dir){
        super(dir);
        OPTIMAL_NUM_LEAVES = Runtime.getRuntime().availableProcessors();
        this.visitedBitSetPool = new GenericObjectPool<>(() -> new BitSet(configuration.maxItemLeaf), nleaves);
        //load all leaves
        for (int i = 0; i < nleaves; i++) {
            leaves[i] = new LeafSegmentWriter(this, i, idxDir);
        }
    }

    private boolean isSafeToCreate(String idxDir){
        File file = new File(idxDir + globalConfigFileName);
        return !IndexUtils.checkFileExist(file);
    }


    private synchronized boolean growLeaf(int idxLeafInAction, boolean isLeafBlocking){
        if (idxLeafInAction == nleaves - 1) {
            System.out.println("Current segment reached maximum capacity, creating and switching to use a new segment.");
            if (leaves.length == nleaves){
                LeafSegment[] hold = leaves;
                leaves = new LeafSegmentWriter[nleaves + 5];
                System.arraycopy(hold, 0, leaves, 0, hold.length);
            }
            if (isLeafBlocking)
                leaves[nleaves] = new LeafSegmentBlockingWriter(this, nleaves,configuration.maxItemLeaf * nleaves++);
            else
                leaves[nleaves] = new LeafSegmentWriter(this, nleaves,configuration.maxItemLeaf * nleaves++);
            return true;
        }
        else if(idxLeafInAction < nleaves - 1){
            return false;
        }
        else
            throw new IllegalArgumentException("In-action leaf's index should not be greater than the number of leaves minus one");
    }
/*
    private synchronized SynchronizedLeafHnswWriter chooseLeaf(){
        if (leaves[nleaves - 1].size() >= configuration.maxItemLeaf){
            if(nleaves < OPTIMAL_NUM_LEAVES){
                return createLeaf();
            }else {
                for (int i = 0; i < OPTIMAL_NUM_LEAVES; i++) {
                    if(leaves[i].size() < leaves[i].maxNodeCount)
                        return leaves[i];
                }
                throw new IllegalArgumentException("Some errors occur when checking capacity");
            }
        }
        else
            return leaves[nleaves - 1];
    }
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


    @Override
    public void addAll(Collection<Item> items) throws InterruptedException {
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
    public void addAll(Collection<Item> items, int numThreads, ProgressListener listener, int progressUpdateInterval) throws InterruptedException {
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

    public void singleSegmentAddAll(Collection<Item> items, int numThreads, ProgressListener listener, int progressUpdateInterval) throws InterruptedException {
        AtomicReference<RuntimeException> throwableHolder = new AtomicReference<>();

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads,
                new NamedThreadFactory("indexer-%d"));

        AtomicInteger workDone = new AtomicInteger();

        try {
            Queue<Item> queue = new LinkedBlockingDeque<>(items);

            CountDownLatch latch = new CountDownLatch(numThreads);
            //final AtomicInteger idxleafInAction = new AtomicInteger(nleaves - 1);
            LeafSegmentBlockingWriter leafInAction = (LeafSegmentBlockingWriter) leaves[nleaves - 1];
            int idxleafInAction = nleaves - 1;
            for (int threadId = 0; threadId < numThreads; threadId++) {

                executorService.submit(() -> {
                    Item item;
                    int idxleaf = idxleafInAction;
                    LeafSegmentBlockingWriter leaf = leafInAction;
                    while(throwableHolder.get() == null && (item = queue.poll()) != null) {
                        try {
                            boolean signal = ((LeafSegmentBlockingWriter)leaf).add(item);
                            if (signal){
                                int done = workDone.incrementAndGet();

                                if (done % progressUpdateInterval == 0) {
                                    listener.updateProgress(done, items.size());
                                }
                            }
                            //here we assume that add(item) return false when
                            //the segment when reached its maximum capacity
                            else {
                                growLeaf(idxleafInAction, true);
                                ++idxleafInAction;
                                ((LeafSegmentBlockingWriter)leaf).add(item);
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

    @Override
    public void save() throws IOException {
        synchronized (configuration){
            Kryo kryo = new Kryo();
            try {
                Output output = new Output(new FileOutputStream(idxDir + globalConfigFileName));
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

    static class InsertItemTask implements Runnable{
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
