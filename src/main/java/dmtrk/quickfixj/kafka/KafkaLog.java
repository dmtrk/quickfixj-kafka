package dmtrk.quickfixj.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Log;
import quickfix.SessionID;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static dmtrk.quickfixj.kafka.KafkaLogFactory.DEFAULT_KAFKA_LOG_KEY_SERIALIZER_CLASS;
import static dmtrk.quickfixj.kafka.KafkaLogFactory.DEFAULT_KAFKA_LOG_VALUE_SERIALIZER_CLASS;

/**
 * Created by dm on 02/11/2018.
 */
public class KafkaLog implements Log, Closeable {
    private final Logger log;
    //
    private final SessionID sessionID;
    private final Lock lock = new ReentrantLock();
    private Producer<Long, String> producer;
    //
    private String bootstrapServers = "127.0.0.1";
    private String messagesTopic = "";
    private String eventsTopic = "";
    private String clientId = getClass() + "@" + hashCode();
    private String keySerializerClass = DEFAULT_KAFKA_LOG_KEY_SERIALIZER_CLASS;
    private String valueSerializerClass = DEFAULT_KAFKA_LOG_VALUE_SERIALIZER_CLASS;

    public KafkaLog(SessionID sessionID) {
        this.sessionID = sessionID;
        this.log = LoggerFactory.getLogger(sessionID + "@" + hashCode());
    }

    private static void shutdown(Producer producer) {
        if (producer != null) try {
            producer.close();
        } catch (Throwable ignored) {
        }
    }

    public SessionID getSessionID() {
        return sessionID;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getMessagesTopic() {
        return messagesTopic;
    }

    public void setMessagesTopic(String messagesTopic) {
        this.messagesTopic = messagesTopic;
    }

    public String getEventsTopic() {
        return eventsTopic;
    }

    public void setEventsTopic(String eventsTopic) {
        this.eventsTopic = eventsTopic;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getKeySerializerClass() {
        return keySerializerClass;
    }

    public void setKeySerializerClass(String keySerializerClass) {
        this.keySerializerClass = keySerializerClass;
    }

    public String getValueSerializerClass() {
        return valueSerializerClass;
    }

    public void setValueSerializerClass(String valueSerializerClass) {
        this.valueSerializerClass = valueSerializerClass;
    }

    public boolean isConnected() {
        return producer != null;
    }

    public void connect(boolean force) throws KafkaException {
        lock.lock();
        try {
            if (force || !isConnected()) {
                shutdown(this.producer);
                //
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
                //
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
                this.producer = new KafkaProducer<>(props);
            }
        } catch (Throwable t) {
            close();
            throw KafkaException.generateException(t);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            shutdown(this.producer);
            this.producer = null;
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        } finally {
            lock.unlock();
        }
    }

    private boolean send(String topic, String message) {
        try {
            if (!isConnected()) {
                connect(false);
            }
            ProducerRecord<Long, String> record = new ProducerRecord<>(topic, message);
            producer.send(record);
            return true;
        } catch (KafkaException e) {
            close();
            log.error(e.getMessage(), e);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        }
        return false;
    }

    @Override
    public void clear() {
        // delete from topic?
    }

    @Override
    public void onIncoming(String message) {
        send(getMessagesTopic(), message);
    }

    @Override
    public void onOutgoing(String message) {
        send(getMessagesTopic(), message);
    }

    @Override
    public void onEvent(String string) {
        send(getEventsTopic(), string);
    }

    @Override
    public void onErrorEvent(String string) {
        send(getEventsTopic(), string);
    }
}
