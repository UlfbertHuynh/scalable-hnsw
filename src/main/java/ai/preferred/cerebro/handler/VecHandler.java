package ai.preferred.cerebro.handler;

import ai.preferred.cerebro.hnsw.Node;
import org.apache.lucene.document.Document;

import java.io.File;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Interface to specify the functions that need detailed implementation
 * for the vector's generic type. This includes:
 *  - How to save and load vectors.
 *  - How to calculate the distance between two given vectors.
 * @param <TVector>
 *
 * @author hpminh@apcs.vn
 */
public interface VecHandler<TVector> {
    /**
     * Extract vectors from an array of {@link Node} then calling the function
     * {@link #save(String, Object[])} to save the vectors. Used by
     * {@link ai.preferred.cerebro.hnsw.LeafSegmentWriter}
     * @param vecFilename path to the file
     * @param nodes the array of nodes the extract vectors from
     * @param nodeCount the actual size of the array, the array of Nodes will be
     *                  allocated with some excess with the intention of adding more in the future
     */
    void saveNodes(String vecFilename, Node<TVector>[] nodes, int nodeCount);

    /**
     * Extract vectors from an {@link AtomicReferenceArray} of {@link Node} then
     * calling the function {@link #save(String, Object[])} to save the vectors.
     * Used by {@link ai.preferred.cgierebro.hnsw.LeafSegmentBlockingWriter}
     * @param vecFilename path to the file
     * @param nodes the array of nodes the extract vectors from
     * @param nodeCount the actual size of the array, the array of Nodes will be allocated
     *                  with some excess with the intention of adding more in the future
     */
    void saveNodesBlocking(String vecFilename, AtomicReferenceArray<Node<TVector>> nodes, int nodeCount);

    /**
     * Saving an array of vectors.
     * @param vecFilename path to the file.
     * @param vecs the array of vectors.
     */
    void save(String vecFilename, TVector[] vecs);

    /**
     * Loading an array of vectors from a file.
     * @param vecsFile a {@link File} object to containing vectors
     * @return vectors
     */
    TVector[] load(File vecsFile);

    /**
     * Function to define the way calculate the distance between two vectors
     * @param a
     * @param b
     * @return distance between two vectors
     */
    double distance(TVector a, TVector b);
}

