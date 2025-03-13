package hu.koromsz.common.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import hu.koromsz.common.kafka.exception.KafkaManagementException;
import hu.koromsz.common.kafka.service.KafkaSenderService;

/**
 * The {@link #KafkaSenderService} has no complex business logic, because it is
 * just a "decorator" around the Kafka sender implementation:
 */
class KafkaSenderServiceTest {

    @Mock
    SendResult<String, String> resultMock;

    @Mock
    CompletableFuture<SendResult<String, String>> futureResultMock;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    RecordMetadata recordMetaDataMock;

    @InjectMocks
    KafkaSenderService kafkaSenderService;

    private static final String TEST_TOPIC = "testTopic";
    private static final String TEST_MESSAGE = "testMessage";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(kafkaTemplate.send(anyString(), anyString())).thenReturn(futureResultMock);
    }

    @Test
    @DisplayName("Send message uses the Kafka sender implementation")
    void sendMessageSendsMessage() throws InterruptedException, ExecutionException, KafkaManagementException {
        when(futureResultMock.get()).thenReturn(resultMock);
        when(resultMock.getRecordMetadata()).thenReturn(recordMetaDataMock);
        when(recordMetaDataMock.hasTimestamp()).thenReturn(Boolean.TRUE);
        kafkaSenderService.sendMessage(TEST_TOPIC, TEST_MESSAGE);
        ArgumentCaptor<String> topicStringCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageStringCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(topicStringCaptor.capture(), messageStringCaptor.capture());
        assertAll(
                () -> assertEquals(TEST_TOPIC, topicStringCaptor.getValue()),
                () -> assertEquals(TEST_MESSAGE, messageStringCaptor.getValue()));
    }

    @Test
    @DisplayName("Sending fails if metadata is empty")
    void throwsExceptionWhenMetadataEmpty() throws InterruptedException, ExecutionException {
        when(futureResultMock.get()).thenReturn(resultMock);
        when(resultMock.getRecordMetadata()).thenReturn(null);
        KafkaManagementException exception = assertThrows(KafkaManagementException.class,
                () -> kafkaSenderService.sendMessage(TEST_TOPIC, TEST_MESSAGE));
        assertAll(
                () -> assertTrue(exception.getMessage().contains(TEST_TOPIC)),
                () -> assertTrue(exception.getMessage().contains(TEST_MESSAGE)));
    }

    /**
     * The thread interrupt call cannot be tested, because the Thead class cannot be
     * mocked.
     */
    @Test
    @DisplayName("Sending handles any exception")
    void sendingHandlesAnyException() {
        InterruptException expectedCauseOfException = new InterruptException("Just an exception");
        when(kafkaTemplate.send(anyString(), anyString())).thenThrow(expectedCauseOfException);
        KafkaManagementException exception = assertThrows(KafkaManagementException.class,
                () -> kafkaSenderService.sendMessage(TEST_TOPIC, TEST_MESSAGE));
        assertAll(
                () -> assertEquals(expectedCauseOfException, exception.getCause()),
                () -> assertTrue(exception.getMessage().contains(TEST_TOPIC)),
                () -> assertTrue(exception.getMessage().contains(TEST_MESSAGE)));
    }
}
