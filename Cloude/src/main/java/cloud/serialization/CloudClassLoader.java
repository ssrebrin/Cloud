package cloud.serialization;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

public class CloudClassLoader extends URLClassLoader {

    public CloudClassLoader(ClassLoader parent) {
        super(new URL[0], parent);
    }

    public void addJar(byte[] jarBytes) throws Exception {
        Path tempJar = Files.createTempFile("cloud-task-", ".jar");
        Files.write(tempJar, jarBytes);
        tempJar.toFile().deleteOnExit();
        addURL(tempJar.toUri().toURL());
    }
}
