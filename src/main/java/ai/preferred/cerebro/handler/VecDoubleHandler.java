package ai.preferred.cerebro.handler;

import ai.preferred.cerebro.hnsw.Node;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * detailed implementation of saving and loading of double vectors.
 * Using {@link Kryo} library.
 *
 * @author hpminh@apcs.vn
 */
public abstract class VecDoubleHandler implements VecHandler<double[]> {

    @Override
    public void saveNodes(String vecFilename, Node<double[]>[] nodes, int nodeCount) {
        double[][] vecs = new double[nodeCount][];
        Node t;
        for (int i = 0; i < nodeCount; i++) {
            t = nodes[i];
            if (t != null)
                vecs[i] = (double[]) t.vector();
            else vecs[i] = null;
        }
        this.save(vecFilename, vecs);
    }

    @Override
    public void saveNodesBlocking(String vecFilename, AtomicReferenceArray<Node<double[]>> nodes, int nodeCount) {
        double[][] vecs = new double[nodeCount][];
        Node t;
        for (int i = 0; i < nodeCount; i++) {
            t = nodes.get(i);
            if (t != null)
                vecs[i] = (double[]) t.vector();
            else vecs[i] = null;
        }
        this.save(vecFilename, vecs);
    }

    @Override
    public void save(String vecFilename, double[][] vecs) {
        Kryo kryo = new Kryo();
        kryo.register(double[].class);
        kryo.register(double[][].class);
        try (Output output = new Output(new FileOutputStream(vecFilename))){
            kryo.writeObject(output, vecs);
        } catch (
                FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public double[][] load(File vecsFile) {
        Kryo kryo = new Kryo();
        kryo.register(double[].class);
        kryo.register(double[][].class);
        try (Input input = new Input(new FileInputStream(vecsFile))) {
            return kryo.readObject(input, double[][].class);
        } catch (
                FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
