package ai.preferred.cerebro.hnsw;


import ai.preferred.cerebro.ConcurrentWriter;
import ai.preferred.cerebro.DistanceFunction;

import ai.preferred.cerebro.IndexUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.stack.mutable.primitive.IntArrayStack;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static ai.preferred.cerebro.IndexConst.Sp;

/**
 * Implementation of {@link ConcurrentWriter} that implements the hnsw algorithm.
 *
 //* @param <T> Type of distance between items (expect any numeric type: float, double, int, ..)
 * @see <a href="https://arxiv.org/abs/1603.09320">
 * Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs</a>
 */
abstract class LeafSegment {
    //constants
    protected final String LOCAL_CONFIG;
    protected final String LOCAL_DELETED;
    protected final String LOCAL_INCONN;
    protected final String LOCAL_OUTCONN;
    protected final String LOCAL_INVERT;
    protected final String LOCAL_VECS;
    //local
    protected String leafName;
    protected int baseID;

    protected volatile int nodeCount;
    protected IntArrayStack freedIds;
    protected volatile Node entryPoint;
    protected Node[] nodes;


    //global - same across all leaves
    protected DistanceFunction distanceFunction;
    protected Comparator<Double> distanceComparator;
    protected int m;
    protected int maxM;//number of connections allowed each node in higher layers
    protected int maxM0;//number of connections allowed each node in base layer - default to twice
    protected double levelLambda; //constant involved in nodes randomizing
    protected int ef;
    protected int efConstruction;
    protected boolean removeEnabled;
    protected int maxNodeCount;

    final protected ParentHnsw parent;
    //<Lucene id, internal id>
    protected ConcurrentHashMap<Integer, Integer> lookup;

    //runtime specific
    public enum Mode{
        CREATE,
        MODIFY /*add, update, delete vectors*/,
        SEARCH
    }
    Mode mode;
    private LeafSegment(ParentHnsw parent,
                        int numName){
        HnswConfiguration configuration = parent.getConfiguration();
        this.maxNodeCount = configuration.maxItemLeaf;
        this.distanceFunction = configuration.distanceFunction;
        this.distanceComparator = configuration.distanceComparator;
        this.m = configuration.m;
        this.maxM = configuration.m;
        this.maxM0 = configuration.m * 2;
        this.levelLambda = 1 / Math.log(this.m);
        this.efConstruction = Math.max(configuration.efConstruction, m);
        this.ef = configuration.ef;
        this.removeEnabled = configuration.removeEnabled;
        this.parent = parent;
        this.lookup = parent.getLookup();
        this.leafName = numName + "_";

        LOCAL_CONFIG = Sp + leafName + "config.o";
        LOCAL_DELETED = Sp + leafName + "deleted.o";
        LOCAL_INCONN = Sp + leafName + "inconns.o";
        LOCAL_OUTCONN = Sp + leafName + "outconns.o";
        LOCAL_INVERT = Sp + leafName + "invert.o";
        LOCAL_VECS = Sp + leafName + "vecs.o";

    }

    //Creation Constructor
    LeafSegment(ParentHnsw parent,
                int numName, int baseID) {
        this(parent, numName);
        this.nodes = new Node[this.maxNodeCount];
        this.freedIds = new IntArrayStack();
        this.baseID = baseID;
        mode = Mode.CREATE;
    }


    //Load constructor
     LeafSegment(ParentHnsw parent,
                 int numName,
                 String idxDir, Mode mode){
        this(parent, numName);
        this.mode = mode;
        load(idxDir);
        /*
        if(mode == Mode.SEARCH)
            this.visitedBitSetPool = new GenericObjectPool<>(() -> new ai.preferred.cerebro.hnsw.BitSet(this.nodeCount), Runtime.getRuntime().availableProcessors());
        else if (mode == Mode.MODIFY)
            this.visitedBitSetPool = new GenericObjectPool<>(() -> new ai.preferred.cerebro.hnsw.BitSet(this.maxNodeCount), Runtime.getRuntime().availableProcessors());
         */
    }

    public int getBaseID() {
        return baseID;
    }

    public int size() {
        synchronized (freedIds) {
            return nodeCount - freedIds.size();
        }
    }

    public Optional<double[]> getVec(int internalID) {
        return Optional.ofNullable(nodes[internalID]).map(Node::vector);
    }

    public Optional<Node> getNode(int internalID) {
        return Optional.ofNullable(nodes[internalID]);
    }

    public int getNodeCount() {
        return nodeCount;
    }

    protected boolean lesser(double x, double y) {
        return distanceComparator.compare(x, y) < 0;
    }

    protected boolean greater(double x, double y) {
        return distanceComparator.compare(x, y) > 0;
    }

    abstract PriorityQueue<Candidate> searchLayer(Node entryPointNode, double[] destination, int k, int layer);

    private boolean checkCorruptedIndex(File configFile, File deletedIdFile,
                                        File inConnectionFile, File outConnectionFile,
                                        File vecsFile, File invertLookUp){
        if(!(IndexUtils.checkFileExist(configFile)
                && IndexUtils.checkFileExist(vecsFile)
                && IndexUtils.checkFileExist(outConnectionFile)
                && IndexUtils.checkFileExist(invertLookUp))) {
            return true;
        }
        if(removeEnabled){
            if(!(IndexUtils.checkFileExist(deletedIdFile) && IndexUtils.checkFileExist(inConnectionFile)))
                return true;
        }
        return false;
    }

    private void load(String dir){
        File configFile = new File(dir + LOCAL_CONFIG);
        File deletedIdFile = new File(dir + LOCAL_DELETED);
        File inConnectionFile = new File(dir + LOCAL_INCONN);
        File outConnectionFile = new File(dir + LOCAL_OUTCONN);
        File vecsFile = new File(dir + LOCAL_VECS);
        File invertLookUpFile = new File(dir + LOCAL_INVERT);

        if (checkCorruptedIndex(configFile, deletedIdFile,
                inConnectionFile, outConnectionFile,
                vecsFile, invertLookUpFile))
            throw new IllegalArgumentException("Index is corrupted");


        int entryID = loadConfig(configFile);
        double[][] vecs = loadVecs(vecsFile);
        IntArrayList[][] outConns = loadConns(outConnectionFile);
        IntArrayList[][] inConns = null;

        int numToLoad = nodeCount;


        if(mode == Mode.MODIFY){
            loadDeletedId(deletedIdFile);
            inConns = loadConns(inConnectionFile);
            numToLoad = maxNodeCount;
        }

        int [] invertLookUp = loadLookup(invertLookUpFile);
        this.nodes = new Node[numToLoad];

        for (int i = 0; i < nodeCount; i++) {
            IntArrayList[] inconn = null;
            if(removeEnabled && mode == Mode.MODIFY)
                inconn = inConns[i];
            if(vecs[i] != null){
                this.nodes[i] = new Node(i,
                                outConns[i],
                                inconn,
                                new Item(invertLookUp[i], vecs[i]));
            }
        }
        this.entryPoint = nodes[entryID];
    }

    //To be handled by parent
    private int[] loadLookup(File lookupFile) {
        int [] lookup = null;
        Kryo kryo = new Kryo();
        kryo.register(int[].class);
        try {
            Input input = new Input(new FileInputStream(lookupFile));
            lookup = kryo.readObject(input, int[].class);
            input.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert nodeCount == lookup.length;
        return lookup;
    }

    private double[][] loadVecs(File vecsFile) {
        double[][] vecs = null;
        Kryo kryo = new Kryo();
        kryo.register(double[].class);
        kryo.register(double[][].class);
        try {
            Input input = new Input(new FileInputStream(vecsFile));
            vecs = kryo.readObject(input, double[][].class);
            input.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert nodeCount == vecs.length;
        return vecs;
    }

    private IntArrayList[][] loadConns(File connFile) {
        IntArrayList[][] conns = null;
        int[][][] data = null;

        Kryo kryo = new Kryo();
        kryo.register(int[].class);
        kryo.register(int[][].class);
        kryo.register(int[][][].class);
        try{
            Input input = new Input(new FileInputStream(connFile));
            data = kryo.readObject(input, int[][][].class);
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert nodeCount == data.length;
        conns = new IntArrayList[data.length][];
        for (int i = 0; i < data.length; i++) {
            conns[i] = new IntArrayList[data[i].length];
            for (int j = 0; j < data[i].length; j++) {
                conns[i][j] = new IntArrayList(data[i][j]);
            }
        }
        return conns;
    }

    private void loadDeletedId(File deletedIdFile) {
        Kryo kryo = new Kryo();
        kryo.register(int[].class);
        kryo.register(IntArrayList.class);
        kryo.register(IntArrayStack.class);
        try {
            Input input = new Input(new FileInputStream(deletedIdFile));
            freedIds = kryo.readObject(input, IntArrayStack.class);
            input.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    //To be handle by parent
    private int loadConfig(File configFile) {
        Kryo kryo = new Kryo();
        Input input = null;
        try {
            input = new Input(new FileInputStream(configFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        baseID = kryo.readObject(input, int.class);
        nodeCount = kryo.readObject(input, int.class);
        //Save the id of entry node
        int entryId = kryo.readObject(input, int.class);
        input.close();
        return entryId;
    }
}
