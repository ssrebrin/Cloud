package cloud.cloud;

import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        RemoteFunction<Integer, Integer> f = x -> x * x;
        Cloud cloud = Cloud.connect("http://localhost:8085");

        List<Integer> result = cloud.execute(f, new int[]{1, 2, 3, 4});
        System.out.println(result);
    }
}
