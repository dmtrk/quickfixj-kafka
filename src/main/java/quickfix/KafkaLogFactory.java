package quickfix;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Created by dm on 02/11/2018.
 */
public class KafkaLogFactory implements LogFactory {
    public static final String SETTING_KAFKA_LOG_BOOTSTRAP_SERVERS = "KafkaLogBootstrapServers";
    public static final String SETTING_KAFKA_LOG_MESSAGES_TOPIC = "KafkaLogMessagesTopic";
    public static final String SETTING_KAFKA_LOG_EVENTS_TOPIC = "KafkaLogEventsTopic";
    public static final String SETTING_KAFKA_LOG_KEY_SERIALIZER_CLASS = "KafkaLogKeySerializerClass";
    public static final String SETTING_KAFKA_LOG_VALUE_SERIALIZER_CLASS = "KafkaLogValueSerializerClass";
    //
    public static final String DEFAULT_KAFKA_LOG_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String DEFAULT_KAFKA_LOG_MESSAGES_TOPIC = "messages";
    public static final String DEFAULT_KAFKA_LOG_EVENTS_TOPIC = "events";
    public static final String DEFAULT_KAFKA_LOG_KEY_SERIALIZER_CLASS = LongSerializer.class.getName();
    public static final String DEFAULT_KAFKA_LOG_VALUE_SERIALIZER_CLASS = StringSerializer.class.getName();
    //
    private final SessionSettings settings;

    public KafkaLogFactory(SessionSettings settings) {
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

    @Override
    /** @deprecated */
    public Log create() {
        return null;
    }

    @Override
    public Log create(SessionID sessionID) {
        try {
            KafkaLog log = new KafkaLog(sessionID);
            log.setBootstrapServers(getStr(settings, sessionID, SETTING_KAFKA_LOG_BOOTSTRAP_SERVERS, DEFAULT_KAFKA_LOG_BOOTSTRAP_SERVERS));
            log.setMessagesTopic(getStr(settings, sessionID, SETTING_KAFKA_LOG_MESSAGES_TOPIC, DEFAULT_KAFKA_LOG_MESSAGES_TOPIC));
            log.setEventsTopic(getStr(settings, sessionID, SETTING_KAFKA_LOG_EVENTS_TOPIC, DEFAULT_KAFKA_LOG_EVENTS_TOPIC));
            log.setClientId(sessionID + "@" + log.hashCode());
            log.setKeySerializerClass(getStr(settings, sessionID, SETTING_KAFKA_LOG_KEY_SERIALIZER_CLASS, DEFAULT_KAFKA_LOG_KEY_SERIALIZER_CLASS));
            log.setValueSerializerClass(getStr(settings, sessionID, SETTING_KAFKA_LOG_VALUE_SERIALIZER_CLASS, DEFAULT_KAFKA_LOG_VALUE_SERIALIZER_CLASS));
            //
            log.connect(true);
            return log;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
