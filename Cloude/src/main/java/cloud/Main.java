package cloud;

import cloud.CloudManager.Network;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Network server = new Network();
        server.run();

        RemoteFunction<Integer,Integer> f = x -> x * x;
        Thread.sleep(3000);
        Cloud c = Cloud.connect("http://localhost:" + "8080");
        System.out.println( c.execute((RemoteFunction<Integer, Integer>) x -> x*x, 10));
    }
}