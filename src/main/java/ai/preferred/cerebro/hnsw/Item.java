package ai.preferred.cerebro.hnsw;

public class Item {
    final int externalId;
    final double[] vector;

    public Item(int externalId, double[] vector) {
        this.externalId = externalId;
        this.vector = vector;
    }

}
