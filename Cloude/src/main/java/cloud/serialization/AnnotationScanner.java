package cloud.serialization;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class AnnotationScanner {

    /**
     * Finds all classes in the current classpath that are annotated with @CloudClass.
     * It scans directories.
     *
     * @return Array of annotated classes.
     */
    public static Class<?>[] findAnnotatedClasses() {
        List<Class<?>> annotatedClasses = new ArrayList<>();
        try {
            String classpath = System.getProperty("java.class.path");
            String separator = System.getProperty("path.separator");
            String[] entries = classpath.split(separator);

            for (String entry : entries) {
                File file = new File(entry);
                if (file.isDirectory()) {
                    scanDirectory(file, "", annotatedClasses);
                }
            }
        } catch (Exception e) {
            System.err.println("Error during annotation scanning: " + e.getMessage());
        }
        return annotatedClasses.toArray(new Class<?>[0]);
    }

    private static void scanDirectory(File directory, String packageName, List<Class<?>> annotatedClasses) {
        File[] files = directory.listFiles();
        if (files == null) return;

        for (File file : files) {
            if (file.isDirectory()) {
                String newPackageName = packageName.isEmpty() ? file.getName() : packageName + "." + file.getName();
                scanDirectory(file, newPackageName, annotatedClasses);
            } else if (file.getName().endsWith(".class")) {
                String className = packageName + "." + file.getName().substring(0, file.getName().length() - 6);
                try {
                    Class<?> clazz = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                    if (clazz.isAnnotationPresent(CloudClass.class)) {
                        annotatedClasses.add(clazz);
                    }
                } catch (ClassNotFoundException | NoClassDefFoundError ignored) {
                }
            }
        }
    }
}
