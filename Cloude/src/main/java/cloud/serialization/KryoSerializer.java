package cloud.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.invoke.SerializedLambda;
import java.util.Arrays;
import java.util.AbstractMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KryoSerializer {
    private static final String LAMBDA_MARKER = "__cloud_lambda__";

    private static final byte[] HEADER_KRYO_V1 = new byte[]{'C', 'L', 'D', 'K', 'R', 'Y', 'O', 1};
    private static final byte[] HEADER_JAVA_V1 = new byte[]{'C', 'L', 'D', 'J', 'A', 'V', 'A', 1};
    private static final int KRYO_READ_TIMEOUT_SECONDS = 20;

    private final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(this::createKryo);

    public KryoSerializer() {
    }

    private Kryo createKryo() {
        Kryo localKryo = new Kryo();
        localKryo.setRegistrationRequired(false);
        localKryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        localKryo.register(SerializedLambda.class);
        localKryo.register(Object[].class);
        localKryo.register(java.lang.Class.class);
        localKryo.register(AbstractMap.SimpleEntry.class, new JavaSerializer());

        // Backward compatibility with previously serialized payloads.
        localKryo.register(LambdaWrapper.class, new JavaSerializer());
        localKryo.register(cloud.serialization.LambdaWrapper.class, new JavaSerializer());
        return localKryo;
    }

    public byte[] serialize(Object obj) {
        try {
            if (obj != null && obj.getClass().getName().contains("$$Lambda")) {
                byte[] payload = javaSerialize(obj);
                return withHeader(HEADER_JAVA_V1, payload);
            }

            byte[] payload = kryoSerialize(obj);
            return withHeader(HEADER_KRYO_V1, payload);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize object: " + e.getMessage(), e);
        }
    }

    public Object deserialize(byte[] data, ClassLoader classLoader) {
        try {
            if (hasHeader(data, HEADER_JAVA_V1)) {
                return javaDeserialize(stripHeader(data, HEADER_JAVA_V1.length), classLoader);
            }

            if (hasHeader(data, HEADER_KRYO_V1)) {
                return unwrapLegacyWrappers(kryoDeserialize(stripHeader(data, HEADER_KRYO_V1.length), classLoader));
            }

            // Legacy payloads without header.
            return unwrapLegacyWrappers(kryoDeserialize(data, classLoader));
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize object: " + e.getMessage(), e);
        }
    }

    private byte[] kryoSerialize(Object obj) {
        Object value = obj;
        if (value != null && value.getClass().getName().contains("$$Lambda")) {
            // Legacy fallback path remains for compatibility if invoked.
            value = new AbstractMap.SimpleEntry<>(LAMBDA_MARKER, value);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryo.get().writeClassAndObject(output, value);
        output.close();
        return outputStream.toByteArray();
    }

    private Object kryoDeserialize(byte[] payload, ClassLoader classLoader) {
        Kryo localKryo = kryo.get();
        localKryo.setClassLoader(classLoader);

        Input input = new Input(new ByteArrayInputStream(payload));
        FutureTask<Object> readTask = new FutureTask<>(() -> localKryo.readClassAndObject(input));
        Thread reader = new Thread(readTask, "kryo-read-guard-" + Thread.currentThread().getId());
        reader.setDaemon(true);
        reader.start();

        try {
            return readTask.get(KRYO_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            reader.interrupt();
            throw new RuntimeException("Kryo deserialization timeout after " + KRYO_READ_TIMEOUT_SECONDS + "s", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Kryo deserialization interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RuntimeException("Kryo deserialization failed: " + cause.getMessage(), cause);
        } finally {
            input.close();
        }
    }

    private byte[] javaSerialize(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }

    private Object javaDeserialize(byte[] payload, ClassLoader classLoader) throws IOException {
        try (ObjectInputStream ois = new ClassLoaderObjectInputStream(new ByteArrayInputStream(payload), classLoader)) {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Class not found during Java deserialization: " + e.getMessage(), e);
        }
    }

    private Object unwrapLegacyWrappers(Object obj) {
        if (obj instanceof LambdaWrapper wrapper) {
            return wrapper.getLambda();
        }
        if (obj instanceof cloud.serialization.LambdaWrapper wrapper) {
            return wrapper.getLambda();
        }
        if (obj instanceof AbstractMap.SimpleEntry<?, ?> wrapper && LAMBDA_MARKER.equals(wrapper.getKey())) {
            return wrapper.getValue();
        }
        return obj;
    }

    private boolean hasHeader(byte[] data, byte[] header) {
        return data != null && data.length >= header.length && Arrays.equals(Arrays.copyOf(data, header.length), header);
    }

    private byte[] stripHeader(byte[] data, int headerLength) {
        return Arrays.copyOfRange(data, headerLength, data.length);
    }

    private byte[] withHeader(byte[] header, byte[] payload) {
        byte[] result = new byte[header.length + payload.length];
        System.arraycopy(header, 0, result, 0, header.length);
        System.arraycopy(payload, 0, result, header.length, payload.length);
        return result;
    }

    private static class ClassLoaderObjectInputStream extends ObjectInputStream {
        private final ClassLoader classLoader;

        private ClassLoaderObjectInputStream(ByteArrayInputStream in, ClassLoader classLoader) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            String name = desc.getName();
            try {
                return Class.forName(name, false, classLoader);
            } catch (ClassNotFoundException ignored) {
                return super.resolveClass(desc);
            }
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
            ClassLoader loader = classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
            Class<?>[] cinterfaces = new Class<?>[interfaces.length];
            for (int i = 0; i < interfaces.length; i++) {
                cinterfaces[i] = Class.forName(interfaces[i], false, loader);
            }
            return java.lang.reflect.Proxy.getProxyClass(loader, cinterfaces);
        }
    }
}
