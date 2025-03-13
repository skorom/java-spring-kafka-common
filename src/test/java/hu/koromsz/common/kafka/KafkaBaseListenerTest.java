package hu.koromsz.common.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import hu.koromsz.common.kafka.service.KafkaBaseListener;

class KafkaBaseListenerTest {

    @Mock
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Mock
    MessageListenerContainer listenerContainer;

    @InjectMocks
    KafkaBaseListener kafkaBaseListener;

    Set<String> dummy_listeners;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        dummy_listeners = Set.of("listener1", "listener2", "listener3");
    }

    @Test
    @DisplayName("A listener can be stopped by its ID")
    void specificListenerCanBeStopped() {
        when(kafkaListenerEndpointRegistry.getListenerContainer(anyString())).thenReturn(listenerContainer);
        assertTrue(kafkaBaseListener.stopListener("pandaListener"));
        verify(listenerContainer, times(1)).stop();
    }

    @Test
    @DisplayName("Stop listener returns false if listener ID not found")
    void stopListenersReturnFalseIfListenerNotFound() {
        when(kafkaListenerEndpointRegistry.getListenerContainer(anyString())).thenReturn(null);
        assertFalse(kafkaBaseListener.stopListener("pandaListener"));
    }

    @Test
    @DisplayName("A listener can be started by its ID")
    void specificListenerCanBeStarted() {
        when(kafkaListenerEndpointRegistry.getListenerContainer(anyString())).thenReturn(listenerContainer);
        assertTrue(kafkaBaseListener.startListener("pandaListener"));
        verify(listenerContainer, times(1)).start();
    }

    @Test
    @DisplayName("Start listener returns false if listener ID not found")
    void startListenersReturnFalseIfListenerNotFound() {
        when(kafkaListenerEndpointRegistry.getListenerContainer(anyString())).thenReturn(null);
        assertFalse(kafkaBaseListener.startListener("pandaListener"));
    }

    @Test
    @DisplayName("Stop all listener calls stop for all  existing IDS")
    void allListenersCanBeStoppedAtOnce() {
        when(kafkaListenerEndpointRegistry.getListenerContainer(anyString())).thenReturn(listenerContainer);
        when(kafkaListenerEndpointRegistry.getListenerContainerIds()).thenReturn(dummy_listeners);
        assertEquals(dummy_listeners, kafkaBaseListener.stopAllListeners());
        verify(listenerContainer, times(3)).stop();
    }

    @Test
    @DisplayName("Start all listener calls start for all existing IDS")
    void allListenersCanBeStartedAtOnce() {
        when(kafkaListenerEndpointRegistry.getListenerContainer(anyString())).thenReturn(listenerContainer);
        when(kafkaListenerEndpointRegistry.getListenerContainerIds()).thenReturn(dummy_listeners);
        assertEquals(dummy_listeners, kafkaBaseListener.startAllListeners());
        verify(listenerContainer, times(3)).start();
    }
}
