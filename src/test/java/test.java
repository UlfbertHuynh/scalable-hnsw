import org.junit.Test;

public class test {

    @Test
    public void whatever(){
        Integer[] arr = {94, 72, 37, 55, 44, 73, 65, 92, 91, 24, 27, 96, 58, 95, 14,  8, 88,
                0, 19, 92, 79, 69, 49, 23, 51, 29,  5, 18, 48,  1};

        heap heap = new heap(20, ()->null);

        for (int i = 0; i < 10; i++) {
            heap.add(arr[i]);
        }
        System.out.println("dfjs;a");
    }
}
