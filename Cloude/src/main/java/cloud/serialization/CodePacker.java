package cloud.serialization;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class CodePacker {

    public static byte[] packClass(Class<?>... classes) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");

        try (JarOutputStream target = new JarOutputStream(baos, manifest)) {
            Set<String> addedEntries = new HashSet<>();
            for (Class<?> clazz : classes) {
                addClassToJar(clazz, target, addedEntries);
            }
        }
        return baos.toByteArray();
    }

    private static void addClassToJar(Class<?> clazz, JarOutputStream target, Set<String> addedEntries) throws IOException {
        String className = clazz.getName();
        String classAsPath = className.replace('.', '/') + ".class";
        
        if (addedEntries.contains(classAsPath)) {
            return;
        }

        try (InputStream is = clazz.getClassLoader().getResourceAsStream(classAsPath)) {
            if (is == null) {
                // Если класс не найден,то мы его просто пропускаем. Для лямбд Kryo не требует байт-кода
                return;
            }

            JarEntry entry = new JarEntry(classAsPath);
            target.putNextEntry(entry);
            byte[] bytes = is.readAllBytes();
            target.write(bytes);
            target.closeEntry();
            addedEntries.add(classAsPath);
        }
    }
    
    // Вспомогательный класс для работы с байтами
    private static class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {}
}
