package hu.koromsz.common.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.retry.backoff.ThreadWaitSleeper;

import hu.koromsz.common.kafka.exception.KafkaManagementException;
import hu.koromsz.common.kafka.service.KafkaConsumerService;

@SuppressWarnings("unchecked")
class KafkaConsumerServiceTest {

    private static final String TEST_TOPIC = "testTopic";
    private static final String TEST_KEY = "testKey";
    private static final String TEST_VALUE = "testValue";
    private static final Integer CONSUMER_TIMEOUT_MILLISECONDS = 1;

    @Mock
    KafkaConsumer<String, String> kafkaConsumer;
    @Mock
    ThreadWaitSleeper sleeper;
    @Mock
    Consumer<String> consumerMock;

    KafkaConsumerService kafkaConsumerService;

    @BeforeEach
    void setUp() throws InterruptedException {
        MockitoAnnotations.openMocks(this);

        kafkaConsumerService = new KafkaConsumerService(kafkaConsumer, CONSUMER_TIMEOUT_MILLISECONDS, sleeper);

        doNothing().when(kafkaConsumer).subscribe(any((Class<List<String>>) (Object) List.class));
        doNothing().when(kafkaConsumer).commitSync(any(Map.class));
        doNothing().when(sleeper).sleep(anyInt());
    }

    @ParameterizedTest(name = "{index} => partitions={0}, messages={1}")
    @MethodSource
    void persistTheIncomingMessagesAndCommit(int numberOfPartitions, int numberOfMessagesInAPartition)
            throws KafkaManagementException {
        int expectedAmountOfMessagesInTotal = numberOfPartitions * numberOfMessagesInAPartition;
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        when(kafkaConsumer.poll(any()))
                .thenReturn(generateConsumerRecords(numberOfPartitions, numberOfMessagesInAPartition))
                .thenReturn(generateConsumerRecords(0, 0));
        Integer amountOfConsumedMessages = kafkaConsumerService.readUnconsumedMessages(consumerMock,
                TEST_TOPIC);
        verify(consumerMock, times(expectedAmountOfMessagesInTotal)).accept(messageCaptor.capture());
        checkIncomingMessagesConsumed(numberOfPartitions, expectedAmountOfMessagesInTotal,
                amountOfConsumedMessages,
                messageCaptor.getAllValues());
    }

    private static Stream<Arguments> persistTheIncomingMessagesAndCommit() {
        return Stream.of(
                Arguments.of(1, 1),
                Arguments.of(1, 2),
                Arguments.of(2, 2));
    }

    @Test
    @DisplayName("Poll with timeout")
    void pollingUsesTheTimeoutFromConfigurationValues() throws KafkaManagementException {
        ArgumentCaptor<Duration> consumerTimeOuCaptor = ArgumentCaptor.forClass(Duration.class);
        when(kafkaConsumer.poll(consumerTimeOuCaptor.capture())).thenReturn(generateConsumerRecords(0,
                0));
        kafkaConsumerService.readUnconsumedMessages(any -> {
        }, TEST_TOPIC);
        assertEquals(CONSUMER_TIMEOUT_MILLISECONDS,
                Integer.valueOf((int) consumerTimeOuCaptor.getValue().toMillis()));
    }

    @Test
    @DisplayName("Retry polling if no message received")
    void isRetrying5TimesIfNoMessageReceived() throws KafkaManagementException, InterruptedException {
        when(kafkaConsumer.poll(any()))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(1, 1))
                .thenReturn(generateConsumerRecords(0, 0));
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        Integer amountOfConsumedMessages = kafkaConsumerService.readUnconsumedMessages(consumerMock,
                TEST_TOPIC);
        verify(consumerMock, times(1)).accept(messageCaptor.capture());
        verify(sleeper, times(5)).sleep(anyLong());
        checkIncomingMessagesConsumed(1, 1, amountOfConsumedMessages, messageCaptor.getAllValues());
    }

    @Test
    @DisplayName("Do nothing when no messages")
    void doNothingWhenThereAreNoMessages() throws KafkaManagementException {
        when(kafkaConsumer.poll(any()))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0))
                .thenReturn(generateConsumerRecords(0, 0));
        Integer amountOfConsumedMessages = kafkaConsumerService.readUnconsumedMessages(consumerMock,
                TEST_TOPIC);
        verifyNoInteractions(consumerMock);
        assertEquals(0, amountOfConsumedMessages);
    }

    @Test
    @DisplayName("Do not commit offset when exception thrown")
    void commitFirstButDoesNotCommitSecondWhenSecondPersistanceFails() {
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsArgumentCaptor = ArgumentCaptor
                .forClass(Map.class);
        when(kafkaConsumer.poll(any()))
                .thenReturn(generateConsumerRecords(1, 1))
                .thenThrow(KafkaException.class);
        KafkaManagementException exception = assertThrows(KafkaManagementException.class,
                () -> kafkaConsumerService.readUnconsumedMessages(consumerMock, TEST_TOPIC));
        verify(consumerMock,
                times(1)).accept(messageCaptor.capture());
        verify(kafkaConsumer, times(1)).commitSync(offsArgumentCaptor.capture());
        assertAll(
                () -> assertEquals(TEST_VALUE + 0, messageCaptor.getValue()),
                () -> assertEquals(1, offsArgumentCaptor.getValue().size()),
                () -> assertEquals(1,
                        offsArgumentCaptor.getValue().values().iterator().next().offset()),
                () -> assertThat(exception.getMessage(), CoreMatchers.containsString(
                        KafkaConsumerService.ERROR_WHILE_CONSUMING_KAFKA_MESSAGES_THE_CONSUMER_DID_NOT_COMMIT_THE_OFFSET)));
    }

    @Test
    @DisplayName("Close kafka connection")
    void closeCallsConsumerClose() {
        kafkaConsumerService.close();
        verify(kafkaConsumer, times(1)).close();

    }

    private void checkIncomingMessagesConsumed(int numberOfPartitions, int expectedAmountOfMessages,
            Integer amountOfConsumedMessages,
            List<String> actualMessages) {
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsArgumentCaptor = ArgumentCaptor
                .forClass(Map.class);
        verify(kafkaConsumer, times(numberOfPartitions)).commitSync(offsArgumentCaptor.capture());
        Object[] expectedValues = reproduceNTimes(expectedAmountOfMessages, i -> TEST_VALUE + i);
        assertAll(
                () -> assertArrayEquals(expectedValues, actualMessages.toArray()),
                () -> assertEquals(expectedAmountOfMessages, amountOfConsumedMessages),
                () -> assertEquals(1, offsArgumentCaptor.getValue().size()),
                () -> assertEquals(expectedAmountOfMessages / numberOfPartitions,
                        offsArgumentCaptor.getValue().values().iterator().next().offset()));
    }

    private ConsumerRecords<String, String> generateConsumerRecords(int numberOfPartitions,
            int numberOfMessagesInAPartition) {
        if (numberOfPartitions < 1) {
            return new ConsumerRecords<>(new HashMap<>());
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();

        int messagesInTotal = 0;
        for (int i = 0; i < numberOfPartitions; i++) {
            TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, i);
            List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
            for (int k = 0; k < numberOfMessagesInAPartition; k++) {
                consumerRecords.add(new ConsumerRecord<>(TEST_TOPIC, i, k,
                        TEST_KEY + messagesInTotal,
                        TEST_VALUE + messagesInTotal));
                messagesInTotal++;
            }
            records.put(topicPartition, consumerRecords);
        }

        return new ConsumerRecords<>(records);
    }

    private String[] reproduceNTimes(int n, IntFunction<String> generator) {
        String[] array = new String[n];
        for (int i = 0; i < n; i++) {
            array[i] = generator.apply(i);
        }
        return array;
    }
}
