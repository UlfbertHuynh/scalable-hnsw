import ai.preferred.cerebro.*;
import ai.preferred.cerebro.hnsw.*;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

public class TestHnsw {

    @Test
    public void testCreateAndSave(){
        double[][] vecs = null;
        String indexDir = IndexConst.HNSW_PATH_MULTI + "4M";
        try {
            vecs = IndexUtils.readVectors(IndexConst.DIM_50_PATH + "itemVec_4M.o");

        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Item> vecList = new ArrayList<>(vecs.length);
        for (int i = 0; i < vecs.length; i++) {
            vecList.add(new Item(i, vecs[i]));
        }
        HnswConfiguration configuration= new HnswConfiguration();
        configuration.setM(20);
        configuration.setEf(20);
        configuration.setEfConstruction(400);
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
    public void testSynchedCreateAndSave(){
        double[][] vecs = null;
        String indexDir = IndexConst.HNSW_PATH_SINGLE + "4M";
        try {
            vecs = IndexUtils.readVectors(IndexConst.DIM_50_PATH + "itemVec_4M.o");

        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Item> vecList = new ArrayList<>(vecs.length);
        for (int i = 0; i < vecs.length; i++) {
            vecList.add(new Item(i, vecs[i]));
        }
        HnswConfiguration configuration= new HnswConfiguration();
        configuration.setM(20);
        configuration.setEf(20);
        configuration.setEfConstruction(400);
        configuration.setMaxItemLeaf(4_000_000);
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
        HnswIndexSearcher index = new HnswIndexSearcher(IndexConst.HNSW_PATH_SINGLE + "4M");
        //HnswIndexSearcher index = new HnswIndexSearcher(IndexConst.HNSW_PATH_MULTI + "4M");
        HashMap<double[], ArrayList<Integer>> queryAndTopK = null;
        try {
            queryAndTopK = IndexUtils.readQueryAndTopK(IndexConst.DIM_50_PATH + "query_top20_4M.o");
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
            double[] query = (double[]) entry.getKey();
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
                System.out.println("Overlapp between brute and and hash (over top 20) is : " + returnIDs.size());
            }
            System.out.println(" ");

        }
        System.out.println("Average search time :" + totalTime/(1000));
        System.out.println("Average overlap :" + totalHit/(1000));

    }
}
