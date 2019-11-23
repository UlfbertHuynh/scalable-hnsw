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
 * detailed implementation of saving and loading of float vectors.
 * Using {@link Kryo} library.
 *
 * @author hpminh@apcs.vn
 */
public abstract class VecFloatHandler implements VecHandler<float[]>  {
    @Override
    public void saveNodes(String vecFilename, Node<float[]>[] nodes, int nodeCount) {
        float[][] vecs = new float[nodeCount][];
        Node t;
        for (int i = 0; i < nodeCount; i++) {
            t = nodes[i];
            if (t != null)
                vecs[i] = (float[]) t.vector();
            else vecs[i] = null;
        }
        this.save(vecFilename, vecs);
    }

    @Override
    public void saveNodesBlocking(String vecFilename, AtomicReferenceArray<Node<float[]>> nodes, int nodeCount) {
        float[][] vecs = new float[nodeCount][];
        Node t;
        for (int i = 0; i < nodeCount; i++) {
            t = nodes.get(i);
            if (t != null)
                vecs[i] = (float[]) t.vector();
            else vecs[i] = null;
        }
        this.save(vecFilename, vecs);
    }

    @Override
    public void save(String vecFilename, float[][] vecs) {
        Kryo kryo = new Kryo();
        kryo.register(float[].class);
        kryo.register(float[][].class);
        try (Output output = new Output(new FileOutputStream(vecFilename))){
            kryo.writeObject(output, vecs);
        } catch (
                FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public float[][] load(File vecsFile) {
        Kryo kryo = new Kryo();
        kryo.register(float[].class);
        kryo.register(float[][].class);
        try (Input input = new Input(new FileInputStream(vecsFile))) {
            return kryo.readObject(input, float[][].class);
        } catch (
                FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
