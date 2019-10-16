package ai.preferred.cerebro.handler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public abstract class VecFloatHandler implements VecHandler<float[]>  {
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
