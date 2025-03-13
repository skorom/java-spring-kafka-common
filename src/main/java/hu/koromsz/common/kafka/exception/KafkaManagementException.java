package hu.koromsz.common.kafka.exception;

/**
 * A custom exception class to handle the errors around the Kafka communication.
 */
public class KafkaManagementException extends Exception {

    public KafkaManagementException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
