package ai.preferred.cerebro.hnsw;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import static ai.preferred.cerebro.IndexConst.Sp;

abstract class ParentHnsw {
    protected final String globalConfigFileName = Sp + "global_config.o";
    protected final String globalLookupFileName = Sp + "global_lookup.o";

    protected String idxDir;
    protected HnswConfiguration configuration;
    protected int nleaves;
    protected ConcurrentHashMap<Integer, Integer> lookup;
    protected GenericObjectPool<BitSet> visitedBitSetPool;
    protected LeafHnsw[] leaves;

    ParentHnsw(){
    }

    //Load Up configuration and lookup table
    ParentHnsw(String dir){
        idxDir = dir;
        Kryo kryo = new Kryo();
        kryo.register(Integer.class);
        kryo.register(ConcurrentHashMap.class);
        //Load up configuration
        Input input = null;
        try {
            input = new Input(new FileInputStream(idxDir + globalConfigFileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        configuration = new HnswConfiguration();
        configuration.setM(kryo.readObject(input, int.class));
        configuration.setEf(kryo.readObject(input, int.class));
        configuration.setEfConstruction(kryo.readObject(input, int.class));
        configuration.setEnableRemove(kryo.readObject(input, boolean.class));
        configuration.setLowMemoryMode(kryo.readObject(input, boolean.class));
        configuration.setMaxItemLeaf(kryo.readObject(input, int.class));
        nleaves = kryo.readObject(input, int.class);
        input.close();
        //Load up lookup table
        try {
            input = new Input(new FileInputStream(idxDir + globalLookupFileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        lookup = kryo.readObject(input, ConcurrentHashMap.class);
        input.close();
    }

    public HnswConfiguration getConfiguration() {
        return configuration;
    }
    public ConcurrentHashMap<Integer, Integer> getLookup(){
        return lookup;
    }
    public BitSet getBitsetFromPool(){
        return visitedBitSetPool.borrowObject();
    }
    public void returnBitsetToPool(BitSet bitSet){
        visitedBitSetPool.returnObject(bitSet);
    }
    public Node getNodeGlobally(int globalID){
        int leafNum = globalID / configuration.maxItemLeaf;
        int internalID = globalID % configuration.maxItemLeaf;
        return leaves[leafNum].getNode(internalID).get();
    }
}
