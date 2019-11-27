import ai.preferred.cerebro.hnsw.IndexUtils;
import ai.preferred.cerebro.handler.DoubleCosineHandler;
import ai.preferred.cerebro.handler.FloatCosineHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Utils {

    public void convert_to_float() throws FileNotFoundException {
        String [] dataSizes = {"1M", "2M", "4M", "6M", "10M"};
        String sampleFile = "itemVec_";
        String queryFile = "query_top20_";
        String fileExt = ".o";
        DoubleCosineHandler doubleHandler = new DoubleCosineHandler();
        FloatCosineHandler floatHandler = new FloatCosineHandler();
        for (String strSize : dataSizes) {
            String srcDataVecs = TestConst.DIM_50_PATH + sampleFile + strSize + fileExt;
            String destDataVecs = TestConst.FLOAT_DATA + sampleFile + strSize + fileExt;
            String srcQuery = TestConst.DIM_50_PATH + queryFile + strSize + fileExt;
            String destQuery = TestConst.FLOAT_DATA + queryFile + strSize + fileExt;

            //convert data files
            double[][] dataVecs = doubleHandler.load(new File(srcDataVecs));
            float [][] convertedData = new float[dataVecs.length][];
            for (int i = 0; i < dataVecs.length; i++)
                convertedData[i] = toFloats(dataVecs[i]);

            floatHandler.save(destDataVecs, convertedData);

            //convert query files
            HashMap<double[], ArrayList<Integer>> queryAndTopK = IndexUtils.readQueryAndTopK(srcQuery);
            HashMap<float[], ArrayList<Integer>> converted_query = new HashMap<>();
            for (Map.Entry<double[], ArrayList<Integer>> entry: queryAndTopK.entrySet()) {
                float[] convertedKey = toFloats(entry.getKey());
                ArrayList<Integer> topK = entry.getValue();
                converted_query.put(convertedKey, topK);
            }
            IndexUtils.saveQueryAndTopKFloat(converted_query, destQuery);
        }
    }

    public float[] toFloats(double[] vec){
        float [] res = new float[vec.length];
        for (int i = 0; i < vec.length; i++)
            res[i] = (float) vec[i];
        return res;
    }

}
