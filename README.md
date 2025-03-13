# Introduction

This library is a general high-level wrapping library around Spring Kafka. You'll get Spring Kafka-related
transitive dependencies, and a few helper classes that you can use for more complicated queue-related tasks.

# Do I need this library?

If Spring Boot is present in your application, the functions of this library may not be useful for you.
In this case, see the linked [KAFKA_4_DUMMIES.md](./KAFKA_4_DUMMIES.md) document on how to connect to a
Kafka broker using simple application properties.

However, if either you don't use Spring Boot, or do use it, but explicitly need one of the services provided
by this library, then you're in the right place.

In short, the library provides the following:

* `KafkaBaseListener`, a base class that exposes various helper methods to interact with the Spring Kafka listener.
* `KafkaConsumerService`, which lets you asynchronously read and consume messages on the queue.
* `KafkaSenderService`, which provides exception checking and logging when sending messages.

# Configuration

Because this library must function without Spring Boot (and because Spring Boot autoconfigures many things),
this library has no configuration class. If you wish to use one of the services, you need to instantiate them
as beans yourself.

Example:

```java
@Bean
KafkaSenderService kafkaSenderService(KafkaTemplate<String, String> kafkaTemplate) {
    return new KafkaSenderService(kafkaTemplate);
}
```

# How to use

In this chapter, we would like to show a few examples on how to use the library.

## Listen to a message

The library provides a base class for your listeners which can be used in the following way:

```java
class YourListener extends KafkaBaseListener {
    @KafkaListener(topics = "whatever_topic_you_need", groupId = "group_id")
    public void persistTransaction(String transaction) {
    }
}
```

The base class provides various common methods that your listener (and your tests) can use, such as starting
and stopping listening on demand.

## Consume message

The "consumption" in this library always means reading the messages from a Kafka broker in an asynchronous way.
To read it real-time (synchronous) we use the "listener" term (see [the section above](#listen-to-a-message)).

The consumption is a bit weird as the Kafka is designed for a real-time publish-subscribe based communication, meaning that the listeners receive and handle the messages as soon as the message arrives to the topic (by the way the topics are not designed to persist the messages).

The library provides an implementation to check the topic and read all the messages that are in the topic at that moment. The only thing that you should do is to use the `KafkaConsumerService` (you may need to create the bean
yourself).

An example:

``` Java
class YourHighLevelService {

    private final YourService yourService;
    private final KafkaConsumerService consumerService;

    public YourHighLevelService(YourService yourService, KafkaConsumerService consumerService) {
        this.yourService=yourService;
        this.consumerService=consumerService;
    }

    public void yourEntryPoint() {
        consumerService.readUnconsumedMessages(yourService::consume,"someTopic");
    }
}

```

**Note**: It is really important to set the `ConsumerConfig.AUTO_OFFSET_RESET_CONFIG` Kafka property
to `earliest` to use the consumption pattern because the default value is `latest` meaning you won't
be able to read messages that was sent before you call the consume method.

**Note2**: The implementation is iterating through the partitions. There is no other way to read all the messages from a topic.

## Send message

If you would like to send a message, you need to create a bean out of `KafkaSenderService`. Afterwards,
you can use it as follows:

``` Java
class YourService {
    private final KafkaSenderService kafkaSenderService;

    public YourService(KafkaSenderService kafkaSenderService){
        this.kafkaSenderService=kafkaSenderService;
    }

    public yourEntryPoint(){
        kafkaSenderService.sendMessage("someTopic","someMessage");
    }
}
```

## Create new Kafka topic

Normally, you do not really need to create a topic by hand, because:

1. In your own local environment you have a full permission of your broker, so the topic will be automatically created if you interact with it (send/subscribe)
2. It is not you who will create the topic in the central Kafka broker. You will have to open a Snow ticket to create a topic and grant  read/write permission to your *ca_* user.

If you still want to do it for some reason. Here is an example code:

``` Java
@Bean
public KafkaAdmin kafkaAdmin() {
    Map<String, Object> configs = new HashMap<>();
    log.debug("The service will use the following Bootstrap Server: {}", configurationValues.getBootstrapAddress());
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configurationValues.getBootstrapAddress());
    return new KafkaAdmin(configs);
}

@Bean
public NewTopic topic1() {
    return new NewTopic("testTopic", 1, (short) 1);
}
```

# How to test

We highly suggest you to use the `org.springframework.kafka:spring-kafka-test` library, because it provides utilities to create an in-memory kafka broker in the integration test phase.

```Java
@SpringBootTest
@EmbeddedKafka(topics = "testTopic", bootstrapServersProperty = "your-server:9092")
class EmbeddedKafkaIT {

}
```

## Testing the message consumption

During the integration testing phase, it is a good practice to stop all the listeners for a while, so that you can fill up the broker with messages. If you do not stop the listeners, just adding some messages to the broker, the listener will immediately consume them. In the following test case, we test if the messages are persisted in the database within a second.

```Java
@Test
@DisplayName("Batch processing of not consumed Kafka messages")
void listenerConsumesPreviousNotHandledMessages() throws Exception {
    kafkaListenerService.stopAllListeners();

    kafkaSenderService.sendMessage("testTopic", "someMessage");
    kafkaSenderService.sendMessage("testTopic", "someMessage2");

    kafkaListenerService.startAllListeners();
    await().atMost(1, TimeUnit.SECONDS).until(() -> !dummyDataDaoServiceImpl.findAll().isEmpty());
    assertTestValuesSavedIntoDatabase(2);
}
```
