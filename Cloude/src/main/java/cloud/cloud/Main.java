package cloud.cloud;


import cloud.cloud.RemoteFunction;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        RemoteFunction<Integer,Integer> f = x -> x * x;
        Thread.sleep(3000);
        Cloud c = Cloud.connect("http://localhost:" + "8085");
        System.out.println( c.execute(f, 10));
    }
}