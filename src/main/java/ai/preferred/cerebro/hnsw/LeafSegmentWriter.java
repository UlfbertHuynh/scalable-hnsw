package ai.preferred.cerebro.hnsw;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.stack.mutable.primitive.IntArrayStack;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;

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
        // globalID is internalID + baseID of the segment
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
                PriorityQueue<Candidate> topCandidates = searchLayer(curNode, newNode.vector(), efConstruction, level);
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
                                           PriorityQueue<Candidate> topCandidates,
                                           int level) {

        int bestN = level == 0 ? this.maxM0 : this.maxM;

        int newNodeId = newNode.internalId;
        double[] newNodeVector = newNode.vector();
        IntArrayList outNewNodeConns = newNode.outConns[level];

        getNeighborsByHeuristic2(topCandidates, null, bestN);

        while (!topCandidates.isEmpty()) {
            int selectedNeighbourId = topCandidates.poll().nodeId;


            outNewNodeConns.add(selectedNeighbourId);

            Node neighbourNode = nodes[selectedNeighbourId];

            if (removeEnabled) {
                neighbourNode.inConns[level].add(newNodeId);
            }

            double[] neighbourVector = neighbourNode.vector();

            IntArrayList outNeighbourConnsAtLevel = neighbourNode.outConns[level];

            //if neighbor also has lower than limit number of connections than just add
            //new connections, no update needed.
            if (outNeighbourConnsAtLevel.size() < bestN) {

                if (removeEnabled) {
                    newNode.inConns[level].add(selectedNeighbourId);
                }

                outNeighbourConnsAtLevel.add(newNodeId);
            }
            // if update is needed:
            // add the new connection to the set of existing ones,
            // then pick out the top limited number allowed, the
            // new conn may be left out or not.
            else {
                double dMax = distanceFunction.distance(newNodeVector, neighbourNode.vector());

                Comparator<Candidate> comparator = Comparator
                        .<Candidate>naturalOrder().reversed();

                PriorityQueue<Candidate> candidates = new PriorityQueue<>(comparator);
                candidates.add(new Candidate(newNodeId, dMax, distanceComparator));

                outNeighbourConnsAtLevel.forEach(id -> {
                    double dist = distanceFunction.distance(neighbourVector, nodes[id].vector());
                    candidates.add(new Candidate(id, dist, distanceComparator));
                });

                MutableIntList prunedConnections = removeEnabled ? new IntArrayList() : null;
                System.out.println(candidates.size());
                System.out.println(bestN);
                getNeighborsByHeuristic2(candidates, prunedConnections, bestN);

                if (removeEnabled) {
                    newNode.inConns[level].add(selectedNeighbourId);
                }


                outNeighbourConnsAtLevel.clear();
                while (!candidates.isEmpty()) {
                    outNeighbourConnsAtLevel.add(candidates.poll().nodeId);
                }

                if (removeEnabled) {
                    prunedConnections.forEach(id -> {
                        Node node = nodes[id];
                        node.inConns[level].remove(selectedNeighbourId);
                    });
                }
            }
        }
    }

    protected void getNeighborsByHeuristic2(PriorityQueue<Candidate> topCandidates,
                                            MutableIntList prunedConnections,
                                            int m) {
        if (topCandidates.size() < m) {
            return;
        }

        PriorityQueue<Candidate> queueClosest = new PriorityQueue<>();
        List<Candidate> returnList = new ArrayList<>();

        while (!topCandidates.isEmpty()) {
            queueClosest.add(topCandidates.poll());
        }

        while (!queueClosest.isEmpty()) {
            Candidate currentPair = queueClosest.poll();

            boolean good;
            if (returnList.size() >= m) {
                good = false;
            } else {
                double distToQuery = currentPair.distance;

                good = true;
                for (Candidate secondPair : returnList) {

                    double curdist = distanceFunction.distance(
                            nodes[secondPair.nodeId].vector(),
                            nodes[currentPair.nodeId].vector()
                    );

                    if (lesser(curdist, distToQuery)) {
                        good = false;
                        break;
                    }

                }
            }
            if (good) {
                returnList.add(currentPair);
            } else {
                if (prunedConnections != null) {
                    prunedConnections.add(currentPair.nodeId);
                }
            }
        }
        topCandidates.addAll(returnList);
    }



    @Override
    PriorityQueue<Candidate> searchLayer(Node entryPointNode, double[] destination, int k, int layer) {
        BitSet visitedBitSet = parent.getBitsetFromPool();
        try {
            PriorityQueue<Candidate> topCandidates =
                    new PriorityQueue<>(Comparator.<Candidate>naturalOrder().reversed());
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

                        if (greater(topCandidates.peek().distance, candidateDistance) || topCandidates.size() < k) {

                            Candidate newCandidate = new Candidate(candidateId, candidateDistance, distanceComparator);

                            checkNeighborSet.add(newCandidate);
                            topCandidates.add(newCandidate);

                            if (topCandidates.size() > k) {
                                topCandidates.poll();
                            }

                            lowerBound = topCandidates.peek().distance;
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
