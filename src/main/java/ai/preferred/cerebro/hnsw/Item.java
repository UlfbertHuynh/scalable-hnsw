package ai.preferred.cerebro.hnsw;

/**
 * Class containing infomations of a sample
 * @param <TVector>
 */
public class Item<TVector> {
    final int externalId;
    final TVector vector;

    public Item(int externalId, TVector vector) {
        this.externalId = externalId;
        this.vector = vector;
    }

}
