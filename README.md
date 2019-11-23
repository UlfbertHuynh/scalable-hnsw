# scalable_hnsw
A Java, scalable implementation of the algorithm described in the paper "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs".

This implementation aims at improving the scalability of the index data structure by adopting a segment-wise approach making it easier to modify (add, delete, update) a built index. It also support dynamic allocation of more space (by creating a new segment) if the insertion operation exceeds the current capacity of the index.

This approach build an index faster by 25% - 30% compared to the original implementation which is one of the main limiting factor of hnsw - a very long build time.

Depending on the machine CPU's number of cores, the accuracy also improved compared to the original single-segment build.

See the code at [test](https://github.com/UlfbertHuynh/scalable-hnsw/tree/master/src/test/java) for example on how to use the library.

This projects adopts the implementation at: https://github.com/jelmerk/hnswlib.
