package ai.preferred.cerebro.handler;

import ai.preferred.cerebro.hnsw.Node;
import org.apache.lucene.document.Document;

import java.io.File;
import java.util.concurrent.atomic.AtomicReferenceArray;

public interface VecHandler<TVector> {
    void saveNodes(String vecFilename, Node<TVector>[] nodes, int nodeCount);
    void saveNodesBlocking(String vecFilename, AtomicReferenceArray<Node<TVector>> nodes, int nodeCount);
    void save(String vecFilename, TVector[] vecs);
    TVector[] load(File vecsFile);
    double distance(TVector a, TVector b);
}

