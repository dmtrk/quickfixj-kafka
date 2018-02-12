package quickfix;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Created by dm on 02/11/2018.
 */
public class KafkaStoreFactory implements MessageStoreFactory {
    public static final String SETTING_KAFKA_STORE_BOOTSTRAP_SERVERS = "KafkaStoreBootstrapServers";
    public static final String SETTING_KAFKA_STORE_TOPIC = "KafkaStoreTopic";
    public static final String SETTING_KAFKA_STORE_KEY_SERIALIZER_CLASS = "KafkaStoreKeySerializerClass";
    public static final String SETTING_KAFKA_STORE_VALUE_SERIALIZER_CLASS = "KafkaStoreValueSerializerClass";
    //
    public static final String DEFAULT_KAFKA_STORE_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String DEFAULT_KAFKA_STORE_TOPIC = "";
    public static final String DEFAULT_KAFKA_STORE_KEY_SERIALIZER_CLASS = IntegerSerializer.class.getName();
    public static final String DEFAULT_KAFKA_STORE_VALUE_SERIALIZER_CLASS = StringSerializer.class.getName();
    //
    private final SessionSettings settings;

    public KafkaStoreFactory(SessionSettings settings) {
        this.settings = settings;
    }

    private static String getStr(SessionSettings settings, SessionID sessionID, String key, String defaultValue) throws FieldConvertError, ConfigError {
        if (settings != null && sessionID != null && key != null) {
            String val = null;
            if (settings.isSetting(sessionID, key)) {
                val = settings.getString(sessionID, key);
            } else if (settings.isSetting(key)) {
                val = settings.getString(key);
            }
            if (val != null) {
                return val.trim();
            }
        }
        return defaultValue;
    }

    public SessionSettings getSettings() {
        return settings;
    }

    @Override
    public KafkaStore create(SessionID sessionID) {
        try {
            KafkaStore store = new KafkaStore(sessionID);
            store.setBootstrapServers(getStr(settings, sessionID, SETTING_KAFKA_STORE_BOOTSTRAP_SERVERS, DEFAULT_KAFKA_STORE_BOOTSTRAP_SERVERS));
            store.setTopic(getStr(settings, sessionID, SETTING_KAFKA_STORE_TOPIC, DEFAULT_KAFKA_STORE_TOPIC));
            store.setClientId(sessionID + "@" + store.hashCode());
            store.setKeySerializerClass(getStr(settings, sessionID, SETTING_KAFKA_STORE_KEY_SERIALIZER_CLASS, DEFAULT_KAFKA_STORE_KEY_SERIALIZER_CLASS));
            store.setValueSerializerClass(getStr(settings, sessionID, SETTING_KAFKA_STORE_VALUE_SERIALIZER_CLASS, DEFAULT_KAFKA_STORE_VALUE_SERIALIZER_CLASS));
            //
            store.connect(true);
            return store;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
