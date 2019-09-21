package ai.preferred.cerebro.hnsw;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.stack.mutable.primitive.IntArrayStack;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

public class LeafSegmentWriter extends LeafSegment {

    //Creation Constructor
    protected LeafSegmentWriter(HnswIndexWriter parent, int numName , int baseID) {
        super(parent, numName, baseID);
    }

    //Load Constructor
    protected LeafSegmentWriter(HnswIndexWriter parent, int numName , String idxDir){
        super(parent, numName, idxDir, Mode.MODIFY);
    }

    protected int assignLevel(Integer value, double lambda) {

        // by relying on the external id to come up with the level, the graph construction should be a lot more stable
        // see : https://github.com/nmslib/hnswlib/issues/28

        int hashCode = value;

        byte[] bytes = new byte[]{
                (byte) (hashCode >> 24),
                (byte) (hashCode >> 16),
                (byte) (hashCode >> 8),
                (byte) hashCode
        };

        double random = Math.abs((double) Murmur3.hash32(bytes) / (double) Integer.MAX_VALUE);

        double r = -Math.log(random) * lambda;
        return (int) r;
    }


    public boolean removeOnInternalID(int internalID) {
        if (!removeEnabled) {
            return false;
        }
        Node node = nodes[internalID];
        for (int level = node.maxLevel(); level >= 0; level--) {
            final int thisLevel = level;
            node.inConns[level].forEach(neighbourId ->
                    nodes[neighbourId].outConns[thisLevel].remove(internalID));

            node.outConns[level].forEach(neighbourId ->
                    nodes[neighbourId].inConns[thisLevel].remove(internalID));
        }

        // change the entry point to the first outgoing connection at the highest level
        if (entryPoint == node) {
            for (int level = node.maxLevel(); level >= 0; level--) {
                IntArrayList outgoingConnections = node.outConns[level];
                if (!outgoingConnections.isEmpty()) {
                    entryPoint = nodes[outgoingConnections.getFirst()];
                    break;
                }
            }

        }
        // if we could not change the outgoing connection it means we are the last node
        if (entryPoint == node) {
            entryPoint = null;
        }
        if (lookup.contains(node.item.externalId))
            lookup.remove(node.item.externalId);
        nodes[internalID] = null;
        freedIds.push(internalID);
        return true;
    }

    public boolean add(Item item) {
        //globalID is internalID + baseID of the segment
        Integer globalId = lookup.get(item.externalId);

        //check if there is nodes with similar id in the graph
        if(globalId != null){
            //if there is similar id but index does not support removal then abort operation
            if (!removeEnabled) {
                return false;
            }
            //if there is already this id in the index, it means this is an update
            //so only handle if this is the leaf that the id was already residing
            if(globalId >= baseID && globalId < baseID + maxNodeCount){
                Node node = nodes[globalId - baseID];
                if (Objects.deepEquals(node.vector(), item.vector)) {
                    //object already added
                    return true;
                } else {
                    //similar id but different vector means different object
                    //so remove the object to insert the current new one
                    removeOnInternalID(item.externalId);
                }
            }
            else
                return false;
        }
        int internalId;
        //try to use used id of deleted node to assign to this new node
        //if not available use a new id (unconsumed) for this node
        if (freedIds.isEmpty()) {
            if (nodeCount >= this.maxNodeCount) {
                return false;
            }
            internalId = nodeCount++;
        } else {
            internalId = freedIds.pop();
        }

        //randomize level
        int randomLevel = assignLevel(item.externalId, this.levelLambda);

        IntArrayList[] outConns = new IntArrayList[randomLevel + 1];

        for (int level = 0; level <= randomLevel; level++) {
            int levelM = randomLevel == 0 ? maxM0 : maxM;
            outConns[level] = new IntArrayList(levelM);
        }

        IntArrayList[] inConns = removeEnabled ? new IntArrayList[randomLevel + 1] : null;
        if (removeEnabled) {
            for (int level = 0; level <= randomLevel; level++) {
                int levelM = randomLevel == 0 ? maxM0 : maxM;
                inConns[level] = new IntArrayList(levelM);
            }
        }

        Node newNode = new Node(internalId, outConns, inConns, item);
        nodes[internalId] = newNode;
        lookup.put(item.externalId, internalId + baseID);

        Node curNode = entryPoint;

        //entry point is null if this is the first node inserted into the graph
        if (curNode != null) {

            //if no layer added
            if (newNode.maxLevel() < entryPoint.maxLevel()) {

                double curDist = distanceFunction.distance(newNode.vector(), curNode.vector());
                //sequentially zoom in until reach the layer next to
                // the highest layer that the new node has to be inserted
                for (int curLevel = entryPoint.maxLevel(); curLevel > newNode.maxLevel(); curLevel--) {

                    boolean changed = true;
                    while (changed){
                        changed = false;
                        IntArrayList candidateConns = curNode.outConns[curLevel];
                        for (int i = 0; i < candidateConns.size(); i++) {

                            int candidateId = candidateConns.get(i);

                            Node candidateNode = nodes[candidateId];

                            double candidateDistance = distanceFunction.distance(newNode.vector(), candidateNode.vector());

                            //updating the starting node to be used at lower level
                            if (lesser(candidateDistance, curDist)) {
                                curDist = candidateDistance;
                                curNode = candidateNode;
                                changed = true;
                            }
                        }
                    }
                }
            }
            //insert the new node starting from its highest layer by setting up connections
            for (int level = Math.min(randomLevel, entryPoint.maxLevel()); level >= 0; level--) {
                //topCandidates hold efConstruction number of nodes closest to the new node in this layer
                //at the top of the heap is the node farthest away from the new node compared to the rest
                //of the heap
                RestrictedMaxHeap topCandidates = searchLayer(curNode, newNode.vector(), efConstruction, level);
                mutuallyConnectNewElement(newNode, topCandidates, level);

            }
        }

        // if this is the first node inserted or its highest layer is higher than that
        // of the current entry node, then we have to update the entry node
        if (entryPoint == null || newNode.maxLevel() > entryPoint.maxLevel()) {
            // this is thread safe because we get the global lock when we add a level
            this.entryPoint = newNode;
        }
        return true;
    }


    protected void mutuallyConnectNewElement(Node newNode,
                                           RestrictedMaxHeap topCandidates,
                                           int level) {

        int bestN = level == 0 ? this.maxM0 : this.maxM;

        int newNodeId = newNode.internalId;
        double[] newNodeVector = newNode.vector();
        IntArrayList outNewNodeConns = newNode.outConns[level];

        //the idea of getNeighborsByHeuristic2() is to introduce a bit of change in which nodes
        //get to connect with our new nodes - not necessary the closest ones. As the authors say
        // in their paper "to make the graph more robust"
        List<Candidate> selectedNeighbors = getNeighborsByHeuristic2(topCandidates, null, bestN);
        for (Candidate selected: selectedNeighbors) {
            int selectedNeighbourId = selected.nodeId;

            outNewNodeConns.add(selectedNeighbourId);

            Node neighbourNode = nodes[selectedNeighbourId];

            if (removeEnabled) {
                neighbourNode.inConns[level].add(newNodeId);
                newNode.inConns[level].add(selectedNeighbourId);
            }

            double[] neighbourVector = neighbourNode.vector();

            IntArrayList outNeighbourConnsAtLevel = neighbourNode.outConns[level];
            //if neighbor also has lower than limit number of connections than just add
            //new connections, no update needed.
            if (outNeighbourConnsAtLevel.size() < bestN) {
                outNeighbourConnsAtLevel.add(newNodeId);
            }
            // if update is needed:
            // add the new connection to the set of existing ones,
            // then pick out the top limited number allowed, the
            // new conn may be left out or not.
            else {

                RestrictedMaxHeap candidates = new RestrictedMaxHeap(bestN, ()-> new Candidate(true));

                outNeighbourConnsAtLevel.forEach(id -> {
                    double dist = distanceFunction.distance(neighbourVector, nodes[id].vector());
                    candidates.updateTop(new Candidate(id, dist, distanceComparator));
                });

                double dis = distanceFunction.distance(newNodeVector, neighbourNode.vector());
                if (greater(candidates.top().distance, dis)) {
                    Candidate ejectedConnection = candidates.top();
                    candidates.updateTop(new Candidate(newNodeId, dis, distanceComparator));
                    outNeighbourConnsAtLevel.clear();
                    outNeighbourConnsAtLevel.addAll(candidates.getCandidateIds());
                    if (removeEnabled) {
                        Node node = nodes[ejectedConnection.nodeId];
                        node.inConns[level].remove(selectedNeighbourId);
                    }
                }
                //MutableIntList prunedConnections = removeEnabled ? new IntArrayList() : null;
                //I don't think we need more robustness at this point as the set is now reduced
                //to bestN + 1 already, and we need to pick out the top bestN. The difference of
                //one candidate doesn't justify calling the costly getNeighborsByHeuristic2() !
                //getNeighborsByHeuristic2(candidates, prunedConnections, bestN);
                /*
                while (!candidates.isEmpty()) {
                    outNeighbourConnsAtLevel.add(candidates.poll().nodeId);
                }

                 */

            }
        }
    }

    //Originally the function return void, we get the selected neighbors in updated
    //topCandidates, this is wasteful as we don't need the data returned to be in the
    //format of a MaxHeap, simply an array will do.
    protected List<Candidate> getNeighborsByHeuristic2(RestrictedMaxHeap topCandidates,
                                                       MutableIntList prunedConnections,
                                                       int m) {
        if (topCandidates.size() < m) {
            return null;
        }

        Stack<Candidate> stackClosest = new Stack<>(topCandidates.size());
        List<Candidate> returnList = new ArrayList<>(m);

        //empty the entire MaxHeap by calling pop()
        //will return the elements in descending order.
        //the original algorithm use a MinHeap to pop
        //out the element in ascending order with each
        //call taking O(log(size)). The same can be achieved
        //if we use a stack but even better as stack has
        //constant time
        while (topCandidates.size() != 0) {
            stackClosest.push(topCandidates.pop());
        }

        while (!stackClosest.isEmpty()) {
            Candidate candidate = stackClosest.pop();

            boolean good;
            if (returnList.size() >= m) {
                good = false;
            } else {
                double distToQuery = candidate.distance;

                good = true;
                for (Candidate chosenOne : returnList) {

                    double curdist = distanceFunction.distance(
                            nodes[chosenOne.nodeId].vector(),
                            nodes[candidate.nodeId].vector()
                    );

                    if (lesser(curdist, distToQuery)) {
                        good = false;
                        break;
                    }

                }
            }
            if (good) {
                returnList.add(candidate);
            } else {
                if (prunedConnections != null) {
                    prunedConnections.add(candidate.nodeId);
                }
            }
        }
        return returnList;
    }



    @Override
    RestrictedMaxHeap searchLayer(Node entryPointNode, double[] destination, int k, int layer) {
        BitSet visitedBitSet = parent.getBitsetFromPool();
        try {
            //a priority queue which can not grow past the initial capacity
            RestrictedMaxHeap topCandidates =
                    new RestrictedMaxHeap(k, ()-> null);

            PriorityQueue<Candidate> checkNeighborSet = new PriorityQueue<>();

            double distance = distanceFunction.distance(destination, entryPointNode.vector());

            Candidate firstCandidade = new Candidate(entryPointNode.internalId, distance, distanceComparator);

            topCandidates.add(firstCandidade);
            checkNeighborSet.add(firstCandidade);
            visitedBitSet.flipTrue(entryPointNode.internalId);

            double lowerBound = distance;

            while (!checkNeighborSet.isEmpty()) {

                Candidate nodeWithNeighbors = checkNeighborSet.poll();

                if (greater(nodeWithNeighbors.distance, lowerBound)) {
                    break;
                }

                MutableIntList candidates = nodes[nodeWithNeighbors.nodeId].outConns[layer];

                for (int i = 0; i < candidates.size(); i++) {

                    int candidateId = candidates.get(i);

                    if (!visitedBitSet.isTrue(candidateId)) {

                        visitedBitSet.flipTrue(candidateId);

                        double candidateDistance = distanceFunction.distance(destination,
                                nodes[candidateId].vector());

                        if (greater(topCandidates.top().distance, candidateDistance) || topCandidates.size() < k) {

                            Candidate newCandidate = new Candidate(candidateId, candidateDistance, distanceComparator);

                            checkNeighborSet.add(newCandidate);
                            if (topCandidates.size() == k)
                                topCandidates.updateTop(newCandidate);
                            else
                                topCandidates.add(newCandidate);

                            lowerBound = topCandidates.top().distance;
                        }
                    }
                }
            }

            return topCandidates;
        } finally {
            visitedBitSet.clear();
            parent.returnBitsetToPool(visitedBitSet);
        }
    }

    public void save(String dir){
        saveConfig(dir);
        saveVecs(dir);
        saveOutConns(dir);
        saveInvertLookUp(dir);
        if(removeEnabled){
            saveDeletedID(dir);
            saveInConns(dir);
        }
    }

    protected void saveInvertLookUp(String dirPath){
        synchronized (nodes){
            int[] invertLookUp = new int[nodeCount];
            for (int i = 0; i < nodeCount; i++) {
                invertLookUp[i] = nodes[i].item.externalId;
            }
            Kryo kryo = new Kryo();
            kryo.register(int[].class);
            try {
                Output outputInvert = new Output(new FileOutputStream(dirPath + LOCAL_INVERT));
                kryo.writeObject(outputInvert, invertLookUp);
                outputInvert.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    protected void saveOutConns(String dirPath) {
        synchronized(nodes){
            int[][][] outConns = new int[nodeCount][][];
            Node t;
            for (int i = 0; i < nodeCount; i++) {
                t = this.nodes[i];
                if(t != null){
                    outConns[i] = new int[t.outConns.length][];
                    for (int j = 0; j < t.outConns.length; j++) {
                        outConns[i][j] = t.outConns[j].toArray();
                    }
                }
                else outConns[i] = null;
            }
            Kryo kryo = new Kryo();
            kryo.register(int[].class);
            kryo.register(int[][].class);
            kryo.register(int[][][].class);
            try {
                Output outputOutconns = new Output(new FileOutputStream(dirPath + LOCAL_OUTCONN));
                kryo.writeObject(outputOutconns, outConns);
                outputOutconns.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    protected void saveInConns(String dirPath) {
        synchronized(nodes){
            int[][][] inConns = new int[nodeCount][][];
            Node t;
            for (int i = 0; i < nodeCount; i++) {
                t = this.nodes[i];
                if(t != null){
                    inConns[i] = new int[t.inConns.length][];
                    for (int j = 0; j < t.inConns.length; j++) {
                        inConns[i][j] = t.inConns[j].toArray();
                    }
                }
                else inConns[i] = null;
            }
            Kryo kryo = new Kryo();
            kryo.register(int[].class);
            kryo.register(int[][].class);
            kryo.register(int[][][].class);
            try {
                Output outputInconns = new Output(new FileOutputStream(dirPath + LOCAL_INCONN));
                kryo.writeObject(outputInconns, inConns);
                outputInconns.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    protected void saveVecs(String dirPath)  {
        synchronized(nodes){
            double[][] vecs = new  double[nodeCount][];
            Node t;
            for (int i = 0; i < nodeCount; i++) {
                t= this.nodes[i];
                if (t != null)
                    vecs[i] = this.nodes[i].vector();
                else vecs[i] = null;
            }
            Kryo kryo = new Kryo();
            kryo.register(double[].class);
            kryo.register(double[][].class);

            try {
                Output outputVec = new Output(new FileOutputStream(dirPath + LOCAL_VECS));
                kryo.writeObject(outputVec, vecs);
                outputVec.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveDeletedID(String dirPath) {
        synchronized (freedIds){
            Kryo kryo = new Kryo();
            kryo.register(int[].class);
            kryo.register(IntArrayList.class);
            kryo.register(IntArrayStack.class);
            try {
                Output output = new Output(new FileOutputStream(dirPath + LOCAL_DELETED));
                kryo.writeObject(output, freedIds);
                output.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveConfig(String dirPath) {
        synchronized (this){
            Kryo kryo = new Kryo();
            try {
                Output output = new Output(new FileOutputStream(dirPath + LOCAL_CONFIG));
                kryo.writeObject(output, baseID);
                kryo.writeObject(output, nodeCount);
                //Save the id of entry node
                kryo.writeObject(output, entryPoint.internalId);
                output.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
    /*
    public void close(){
        synchronized (this){
            freedIds = null;
            entryPoint = null;
            nodes = null;
            lookup = null;
        }
    }
     */
}
