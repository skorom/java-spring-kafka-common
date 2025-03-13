package hu.koromsz.common.kafka.service;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * A class that has some useful general kafka listener specific implementations.
 * Currently, it means the listener starter and stopper mechanism.
 * It can be useful in the integration test phase, because you can temporarily
 * disable your listeners, so that you can fill up your Kafka broker with
 * messages.
 */
public class KafkaBaseListener {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    public KafkaBaseListener(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    /**
     * Starts a specific Kafka listener.
     *
     * @param listenerId the id of the listener
     * @return true if the listener started
     */
    public boolean startListener(String listenerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);
        if (listenerContainer == null) {
            return false;
        }
        listenerContainer.start();
        log.debug("{} Kafka Listener Started", listenerId);
        return true;
    }

    /**
     * Stops a specific Kafka listener.
     *
     * @param listenerId the id of the listener
     * @return true if the listener stopped
     */
    public Boolean stopListener(String listenerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);
        if (listenerContainer == null) {
            return Boolean.FALSE;
        }
        listenerContainer.stop();
        log.debug("{} Kafka Listener Stopped.", listenerId);
        return Boolean.TRUE;
    }

    /**
     * Stop all Kafka listeners.
     *
     * @return with the set of stopped listener ids.
     */
    public Set<String> stopAllListeners() {
        Set<String> stoppedIds = this.manageAllListeners(this::stopListener);
        if (log.isDebugEnabled()) {
            log.debug("The following listeners stopped: {}", stoppedIds.stream().collect(Collectors.joining(",")));
        }
        return stoppedIds;
    }

    /**
     * Start all Kafka listeners
     *
     * @return wit the set of started listener ids.
     */
    public Set<String> startAllListeners() {
        Set<String> startedIds = this.manageAllListeners(this::startListener);
        if (log.isDebugEnabled()) {
            log.debug("The following listeners started: {}", startedIds.stream().collect(Collectors.joining(",")));
        }
        return startedIds;
    }

    private Set<String> manageAllListeners(Predicate<String> function) {
        return manageListeners(function, kafkaListenerEndpointRegistry.getListenerContainerIds());

    }

    private Set<String> manageListeners(Predicate<String> function, Set<String> listeners) {
        Set<String> managedIds = new HashSet<>();
        listeners.forEach(listenerId -> {
            Boolean isManaged = function.test(listenerId);
            if (Boolean.TRUE.equals(isManaged)) {
                managedIds.add(listenerId);
            }
        });

        return managedIds;
    }
}
