package ai.preferred.cerebro;

import org.apache.lucene.document.Document;

public interface VecHandler<TVector> {
    void save(String vecFilename, TVector[] vecs);
    TVector[] load(String vecFilename);
    double distance(TVector a, TVector b);
}

