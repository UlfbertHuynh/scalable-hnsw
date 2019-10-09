package ai.preferred.cerebro.hnsw;

import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

public class Node<TVector> {
    final int internalId;

    final IntArrayList[] outConns;

    final IntArrayList[] inConns;

    final Item<TVector> item;


    Node(int id, IntArrayList[] outgoingConnections, IntArrayList[] incomingConnections, Item item) {
        assert item != null;
        this.internalId = id;
        this.outConns = outgoingConnections;
        this.inConns = incomingConnections;
        this.item = item;
    }

    int maxLevel() {
        return this.outConns.length - 1;
    }

    int externalID(){
        return item.externalId;
    }

    TVector vector(){
        return item.vector;
    }

}
