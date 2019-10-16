package ai.preferred.cerebro.handler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public abstract class VecDoubleHandler implements VecHandler<double[]> {
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
