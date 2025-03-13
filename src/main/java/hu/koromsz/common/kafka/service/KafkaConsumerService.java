package hu.koromsz.common.kafka.service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.backoff.Sleeper;
import org.springframework.retry.backoff.ThreadWaitSleeper;

import hu.koromsz.common.kafka.exception.KafkaManagementException;

/**
 * An implementation to consume Kafka message in an asynchronous way.
 * It means that when you fire the consumer functionality, it will read all the
 * messages at once.
 */
public class KafkaConsumerService {

    public static final String ERROR_WHILE_CONSUMING_KAFKA_MESSAGES_THE_CONSUMER_DID_NOT_COMMIT_THE_OFFSET = "Error while consuming Kafka messages. The consumer did not commit the offset.";

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final long consumerTimeout;
    private final Sleeper sleeper;

    public KafkaConsumerService(KafkaConsumer<String, String> kafkaConsumer,
            long consumerTimeout, ThreadWaitSleeper sleeper) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumerTimeout = consumerTimeout;
        this.sleeper = sleeper;
    }

    /**
     * <p>
     * Read all the unconsumed messages from a particular topic.
     * It will not consume all the messages at once, but in smaller packages.
     * The batch size is configurable to avoid memory leak, but the function will
     * not stop until everything is consumed (so theoretically an infinite loop is
     * possible).
     * </p>
     * <p>
     * It is really important to mention that the offset is not automatically set
     * here. If something fails, the offset will not be committed, so the message
     * can be consumed again by the group.
     * </p>
     * <p>
     * Another important note is that the Kafka connection will not be closed after
     * the consumption, because you many want to read some messages again. Close the
     * connection manually by using {@link #close()} function but be very careful.
     * The closure does not close properly, some threads remain in the execution.
     * </p>
     * <p>
     * It is possible that the consumer poll returns with empty records, because the
     * offset configuration is done (it is asynchronous).
     * To make sure that we consume everything, the function will retry the polling
     * mechanism
     * 5 times if they were empty.
     * </p>
     *
     * @param kafkaMessageConsumer a method that will be called with the consumed
     *                             message string (as many as times as many messages
     *                             we have in the topic)
     * @param topic                the topic that the consumer subscribes to
     * @return the amount of consumed messages
     * @throws KafkaManagementException when something happens during the
     *                                  consumption
     */
    public Integer readUnconsumedMessages(Consumer<String> kafkaMessageConsumer, String topic)
            throws KafkaManagementException {
        return readUnconsumedMessages(kafkaMessageConsumer, Arrays.asList(topic));
    }

    /**
     * <p>
     * Read all the unconsumed messages from a particular topic.
     * It will not consume all the messages at once, but in smaller packages.
     * The batch size is configurable to avoid memory leak, but the function will
     * not stop until everything is consumed (so theoretically an infinite loop is
     * possible).
     * </p>
     * <p>
     * It is really important to mention that the offset is not automatically set
     * here (not like in the {@link #receiveMessage} above). If something fails,
     * the offset will not be committed, so the message can be consumed again by the
     * group.
     * </p>
     * <p>
     * Another important note is that the Kafka connection will not be closed after
     * the consumption, because you many want to read some messages again. Close the
     * connection manually by using {@link #close()} function. The closure does not
     * close properly, some threads remain in the execution.
     * </p>
     * <p>
     * It is possible that the consumer poll returns with empty records, because the
     * offset configuration is done (it is asynchronous).
     * To make sure that we consume everything, the function will retry the polling
     * mechanism
     * 5 times if they were empty.
     * </p>
     *
     * @param kafkaMessageConsumer a method that will be called with the consumed
     *                             message string (as many as times as many messages
     *                             we have in the topic)
     * @param topics               the topics that the consumer subscribes to
     * @return the amount of consumed messages
     * @throws KafkaManagementException when something happens during the
     *                                  consumption
     */
    public Integer readUnconsumedMessages(Consumer<String> kafkaMessageConsumer, Collection<String> topics)
            throws KafkaManagementException {
        String topicString = topics.stream().collect(Collectors.joining(","));
        log.debug("Reading unconsumed messages in a synchronous way from: {}",
                topicString);
        Integer amountOfConsumedMessages = 0;
        try {
            kafkaConsumer.subscribe(topics);
            boolean shouldConsume = true;
            Integer emptyTimes = 1;
            do {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(consumerTimeout));
                shouldConsume = !records.isEmpty();
                if (shouldConsume) {
                    log.debug(
                            "Another iteration, because there are still messages in the queue. Current amount of consumed messages: {}",
                            amountOfConsumedMessages);
                    amountOfConsumedMessages += consumeRecords(records, kafkaMessageConsumer);

                } else {
                    if (emptyTimes <= 5) {
                        log.debug("Waiting for the {}. time, because there are no messages in the topic.", emptyTimes);
                        emptyTimes++;
                        sleeper.sleep(1000);
                        shouldConsume = true;
                    }
                }
            } while (shouldConsume);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaManagementException(
                    ERROR_WHILE_CONSUMING_KAFKA_MESSAGES_THE_CONSUMER_DID_NOT_COMMIT_THE_OFFSET, ex);
        } catch (Exception ex) {
            throw new KafkaManagementException(
                    ERROR_WHILE_CONSUMING_KAFKA_MESSAGES_THE_CONSUMER_DID_NOT_COMMIT_THE_OFFSET, ex);
        }
        log.info("Consumed {} amount of messages in total. End of consuming.",
                amountOfConsumedMessages);
        return amountOfConsumedMessages;
    }

    private Integer consumeRecords(ConsumerRecords<String, String> records, Consumer<String> kafkaMessageConsumer) {
        Integer amountOfConsumedMessages = 0;
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                log.debug("The following message consumed: {}; offset:{}; partition:{}",
                        partitionRecord.value(),
                        partitionRecord.offset(), partitionRecord.partition());
                kafkaMessageConsumer.accept(partitionRecord.value());
                amountOfConsumedMessages++;
            }
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            kafkaConsumer
                    .commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
        }
        return amountOfConsumedMessages;
    }

    /**
     * Close the kafka connection.
     */
    public void close() {
        kafkaConsumer.close();
    }
}