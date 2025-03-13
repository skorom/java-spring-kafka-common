package hu.koromsz.common.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import hu.koromsz.common.kafka.exception.KafkaManagementException;

/**
 * A service to send kafka messages.
 */
public class KafkaSenderService {

    public static final String UNABLE_TO_SEND_MESSAGE = "Unable to send message='%s' to topic='%s'";

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public KafkaSenderService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends a message to a kafka topic.
     * Note: The whole service will use the "foo" group ID
     *
     * @param topicName the topic name
     * @param message   the message
     * @throws KafkaManagementException if it was unable to send the message
     */
    public void sendMessage(String topicName, String message) throws KafkaManagementException {
        try {
            SendResult<String, String> result = kafkaTemplate.send(topicName, message).get();
            if (result.getRecordMetadata() != null && result.getRecordMetadata().hasTimestamp()) {
                log.debug("Sent message={} to topic={} with offset={}", message, topicName,
                        result.getRecordMetadata().offset());
            } else {
                throw new KafkaManagementException(
                        String.format(UNABLE_TO_SEND_MESSAGE, message, topicName), null);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaManagementException(
                    String.format(UNABLE_TO_SEND_MESSAGE, message, topicName), ex);
        } catch (Exception ex) {
            throw new KafkaManagementException(
                    String.format(UNABLE_TO_SEND_MESSAGE, message, topicName), ex);
        }
    }
}
