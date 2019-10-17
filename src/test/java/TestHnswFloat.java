import ai.preferred.cerebro.IndexUtils;
import ai.preferred.cerebro.handler.DoubleCosineHandler;
import ai.preferred.cerebro.handler.FloatCosineHandler;
import ai.preferred.cerebro.hnsw.*;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TestHnswFloat {
    @Test
    public void testPrintInfo(){
        String indexDir = TestConst.HNSW_PATH_MULTI + "1M";
        ParentHnsw.printIndexInfo(indexDir);
    }

    @Test
    public void testCreateAndSave(){
        float[][] vecs = null;
        String indexDir = TestConst.HNSW_PATH_MULTI + "1M";
        FloatCosineHandler handler = new FloatCosineHandler();
        vecs = handler.load(new File(TestConst.FLOAT_DATA + "itemVec_1M.o"));

        List<Item<float[]>> vecList = new ArrayList<>(vecs.length);
        for (int i = 0; i < vecs.length; i++) {
            vecList.add(new Item<>(i, vecs[i]));
        }
        HnswConfiguration configuration= new HnswConfiguration(handler);
        configuration.setM(50);
        configuration.setEf(50);
        configuration.setEfConstruction(500);
        HnswIndexWriter<float[]> index = new HnswIndexWriter<>(configuration, indexDir);

        try {
            index.addAll(vecList);
            index.save();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSynchedCreateAndSave(){
        double[][] vecs = null;
        String indexDir = TestConst.HNSW_PATH_SINGLE + "1M";
        try {
            vecs = IndexUtils.readVectors(TestConst.FLOAT_DATA + "itemVec_1M.o");

        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Item<double[]>> vecList = new ArrayList<>(vecs.length);
        for (int i = 0; i < vecs.length; i++) {
            vecList.add(new Item<>(i, vecs[i]));
        }
        HnswConfiguration configuration= new HnswConfiguration(new DoubleCosineHandler());
        configuration.setM(50);
        configuration.setEf(50);
        configuration.setEfConstruction(500);
        //configuration.setMaxItemLeaf(500_000);
        configuration.setLowMemoryMode(true);
        HnswIndexWriter index = new HnswIndexWriter(configuration, indexDir);

        try {
            index.addAll(vecList);
            index.save();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoadAndSearch(){
        //HnswIndexSearcher index = new HnswIndexSearcher(IndexConst.HNSW_PATH_SINGLE + "1M");
        HnswIndexSearcher index = new HnswIndexSearcher(TestConst.HNSW_PATH_MULTI + "1M");
        HashMap<float[], ArrayList<Integer>> queryAndTopK = null;
        try {
            queryAndTopK = IndexUtils.readQueryAndTopKFloat(TestConst.FLOAT_DATA + "query_top20_1M.o");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        double totalTime = 0;
        double totalHit = 0;
        System.out.println("Load Complete");
        Iterator it = queryAndTopK.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry entry = (Map.Entry) it.next();
            float[] query = (float[]) entry.getKey();
            ArrayList<Integer> setBrute = (ArrayList<Integer>) entry.getValue();
            long startTime = System.currentTimeMillis();
            TopDocs res = index.search(query, 20);
            long endSearchTime = System.currentTimeMillis();
            System.out.println("Top-20 query time: " +(endSearchTime-startTime)+" ms");
            totalTime += endSearchTime - startTime;
            ArrayList<Integer> returnIDs = new ArrayList<>();
            for (int i = 0; i < res.scoreDocs.length; i++) {
                returnIDs.add(res.scoreDocs[i].doc);
            }
            if(returnIDs.retainAll(setBrute)){
                totalHit += returnIDs.size();
                System.out.println("Overlapp between brute and hash (over top 20) is : " + returnIDs.size());
            }
            System.out.println(" ");
        }
        System.out.println("Average search time :" + totalTime/(1000));
        System.out.println("Average overlap :" + totalHit/(1000));

    }
}
