# Scalable_hnsw
A Java, scalable implementation of the algorithm describe in the paper "Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs"

The original source code can be found at: https://github.com/jelmerk/hnswlib.

This implementation aims at improving the scalability of the index data structure by adopting a segment-wise approach making it easier to modify (add, delete, updata) a built index.

This approach build an index faster by 25% - 30% compared to the original implementation which is one of the main limiting factor of hnsw - a very long build time.
