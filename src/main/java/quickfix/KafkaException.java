package quickfix;

import java.io.IOException;

/**
 * Created by dm on 02/11/2018.
 */
public class KafkaException extends IOException {
    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaException(Throwable cause) {
        super(cause);
    }

    public static KafkaException generateException(Throwable t) {
        if (t instanceof KafkaException) {
            return (KafkaException) t;
        } else {
            return new KafkaException(t);
        }
    }
}
