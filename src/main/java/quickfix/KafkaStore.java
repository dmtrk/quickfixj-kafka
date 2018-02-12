package quickfix;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static quickfix.KafkaStoreFactory.DEFAULT_KAFKA_STORE_KEY_SERIALIZER_CLASS;
import static quickfix.KafkaStoreFactory.DEFAULT_KAFKA_STORE_VALUE_SERIALIZER_CLASS;

/**
 * Created by dm on 02/11/2018.
 */
public class KafkaStore implements MessageStore, Closeable {
    private final Logger log;
    //
    private final SessionID sessionID;
    private final Lock lock = new ReentrantLock();
    private Producer<Integer, String> producer;
    //
    private String bootstrapServers = "127.0.0.1";
    private String topic = "";
    private String clientId = getClass() + "@" + hashCode();
    private String keySerializerClass = DEFAULT_KAFKA_STORE_KEY_SERIALIZER_CLASS;
    private String valueSerializerClass = DEFAULT_KAFKA_STORE_VALUE_SERIALIZER_CLASS;

    public KafkaStore(SessionID sessionID) {
        this.sessionID = sessionID;
        this.log = LoggerFactory.getLogger(sessionID + "@" + hashCode());
    }

    private static void shutdown(Producer producer) {
        if (producer != null) try {
            producer.close();
        } catch (Throwable ignored) {
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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
        try {
            if (force || !isConnected()) {
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
        }
    }

    @Override
    public void close() {
        try {
            shutdown(this.producer);
            this.producer = null;
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        }
    }

    @Override
    public boolean set(int sequence, String message) throws KafkaException {
        try {
            if (!isConnected()) {
                connect(false);
            }
            ProducerRecord<Integer, String> record = new ProducerRecord<>(getTopic(), sequence, message);
            producer.send(record);
            return true;
        } catch (KafkaException e) {
            close();
            throw e;
        } catch (Throwable t) {
            throw KafkaException.generateException(t);
        }
    }

    @Override
    public void get(int startSequence, int endSequence, Collection<String> messages) throws IOException {
        // not implemented
    }

    @Override
    public int getNextSenderMsgSeqNum() throws IOException {
        return 0;
    }

    @Override
    public void setNextSenderMsgSeqNum(int next) throws IOException {
        // not implemented
    }

    @Override
    public int getNextTargetMsgSeqNum() throws IOException {
        return 0;
    }

    @Override
    public void setNextTargetMsgSeqNum(int next) throws IOException {
        // not implemented
    }

    @Override
    public void incrNextSenderMsgSeqNum() throws IOException {
        // not implemented
    }

    @Override
    public void incrNextTargetMsgSeqNum() throws IOException {
        // not implemented
    }

    @Override
    public Date getCreationTime() throws IOException {
        return new Date();
    }

    @Override
    public void reset() throws IOException {
        // not implemented
    }

    @Override
    public void refresh() throws IOException {
        // not implemented
    }
}
