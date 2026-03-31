package cloud.cloud;

import java.io.IOException;
import java.util.List;

public class MainClojure {
    public static void main(String[] args) throws IOException, InterruptedException {
        ClojureCloud clojureCloud = ClojureCloud.connect("http://localhost:8085");

        List<Integer> legacyResult = clojureCloud.execute("(fn [x] (* x x))", new int[]{2, 3, 4, 5});
        System.out.println("[clojure][legacy] squares: " + legacyResult);

        List<Object> streamResult = ClojureCloudStream.connect("http://localhost:8085")
                .stream(new int[]{1, 2, 3, 4, 5, 6})
                .filter("(fn [x] (zero? (mod x 2)))")
                .map("(fn [x] (* x 10))")
                .reduce("(fn [a b] (+ a b))")
                .execute();

        System.out.println("[clojure][pipeline] even*10 sum: " + streamResult);
    }
}
