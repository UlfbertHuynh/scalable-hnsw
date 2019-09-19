package ai.preferred.cerebro;

import org.apache.commons.lang3.SystemUtils;

public class IndexConst {
    public final static String DIM_50_PATH = "C:\\50_dims_testing\\";
    public final static String HNSW_PATH_SINGLE = "E:\\hnsw_single_segment\\";
    public final static String HNSW_PATH_MULTI = "E:\\hnsw_multi_segment\\";

    //Reserved keywords to avoid using as fieldname
    public final static String IDFieldName = "ID";
    public final static String VecFieldName = "Feature_Vector";
    public final static String VecLenFieldName = "Vec_Length";
    public final static String HashFieldName = "LSH_Hash_Code";
    //These are not reserved keywords but you should
    //understand how Cerebro handle text file by default
    public static final String CONTENTS = "contents";
    public static final String FilePathField = "path_to_file";
    public static final long mb = 1 << 20;

    public static final char Sp = SystemUtils.IS_OS_WINDOWS ? '\\' : '/';
}