import ai.preferred.cerebro.handler.DoubleCosineHandler;
import ai.preferred.cerebro.handler.FloatCosineHandler;
import ai.preferred.cerebro.handler.VecHandler;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;

public class test {

    //@Test
    public void whatever(){
        Integer[] arr = {94, 72, 37, 55, 44, 73, 65, 92, 91, 24, 27, 96, 58, 95, 14,  8, 88,
                0, 19, 92, 79, 69, 49, 23, 51, 29,  5, 18, 48,  1};

        heap heap = new heap(20, ()->null);

        for (int i = 0; i < 10; i++) {
            heap.add(arr[i]);
        }
        System.out.println("dfjs;a");
    }

    //@Test
    public void testSaveClass() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        VecHandler handler = new DoubleCosineHandler();
        String filename = "vecHandler.o";
        String className = handler.getClass().getCanonicalName();
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor = clazz.getConstructor();
        VecHandler object = (VecHandler) constructor.newInstance(new Object[] {});
        /*
        Kryo kryo = new Kryo();
        kryo.register(DoubleCosineHandler.class.c);
        kryo.register(FloatCosineHandler.class);
        try {
            Output output = new Output(new FileOutputStream(filename));
            kryo.writeObject(output, handler);
            output.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

         */
    }

    //@Test
    public void testLoadClass(){
        String filename = "vecHandler.o";
        Kryo kryo = new Kryo();
        kryo.register(VecHandler.class);
        kryo.register(DoubleCosineHandler.class);
        kryo.register(FloatCosineHandler.class);
        Input input = null;
        try {
            input = new Input(new FileInputStream(filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        VecHandler handler= kryo.readObject(input, VecHandler.class);
        input.close();
    }
}
