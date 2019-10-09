package ai.preferred.cerebro.hnsw;

public class Item<TVector> {
    final int externalId;
    final TVector vector;

    public Item(int externalId, TVector vector) {
        this.externalId = externalId;
        this.vector = vector;
    }

}
