package ai.preferred.cerebro.handler;

import org.apache.lucene.document.Document;

import java.io.File;

public interface VecHandler<TVector> {
    void save(String vecFilename, TVector[] vecs);
    TVector[] load(File vecsFile);
    double distance(TVector a, TVector b);
}

