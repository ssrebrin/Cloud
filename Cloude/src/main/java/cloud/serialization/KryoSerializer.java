package cloud.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.invoke.SerializedLambda;

public class KryoSerializer {

    private final Kryo kryo;

    public KryoSerializer() {
        this.kryo = new Kryo();
        this.kryo.setRegistrationRequired(false);
        this.kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        
        // Регистрация базовых типов
        this.kryo.register(SerializedLambda.class);
        this.kryo.register(Object[].class);
        this.kryo.register(java.lang.Class.class);
        
        // Регистрация обертки с принудительным использованием JavaSerializer
        this.kryo.register(LambdaWrapper.class, new JavaSerializer());
    }

    public byte[] serialize(Object obj) {
        // Если это лямбда, оборачиваем её в наш LambdaWrapper
        if (obj != null && obj.getClass().getName().contains("$$Lambda")) {
            obj = new LambdaWrapper(obj);
        }
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryo.writeClassAndObject(output, obj);
        output.close();
        return outputStream.toByteArray();
    }

    public Object deserialize(byte[] data, ClassLoader classLoader) {
        kryo.setClassLoader(classLoader);
        Input input = new Input(new ByteArrayInputStream(data));
        Object obj = kryo.readClassAndObject(input);
        input.close();
        
        // Если получили обертку, достаем из неё настоящую лямбду
        if (obj instanceof LambdaWrapper) {
            return ((LambdaWrapper) obj).getLambda();
        }
        
        return obj;
    }
}
