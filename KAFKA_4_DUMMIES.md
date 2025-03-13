# Kafka for dummies

A compact reference guide on how to use Spring Kafka in a Spring Boot environment for simply listening to a given
Kafka topic.

## Preamble

Spring Kafka in and of itself does not configure much, it requires that *you* set up all the necessary configuration
beans in order for everything to work. Spring Boot however, does all this heavylifting behind the scenes, all you
need to do is specify the configuration variables in your application properties file, put some annotations on your
listener method, and *bam*, you're done.

For exactly this reason, pulling an entire library in might seem counterintuitive. In simple cases, it might be, but
be sure to check out the [Readme](./README.md) to see our recommendations on this.

## Configuration - non-secure Kafka

If you need a plaintext connection, you only
need one short configuration line in your `application.properties` or `application.yaml` file.

```yaml
spring.kafka.bootstrap-servers: my-kafka-broker-host:9092
```

In some cases, you may have a secure Kafka connection usually, and want to override that in the local configuration.
To do this, simply add the following two lines to disable all previously configured security setup:

```yaml
spring.kafka.security.protocol: PLAINTEXT
spring.kafka.jaas.enabled: false
```

That's about it! See the below section about setting up the listener method itself to finish integrating everything.

## Configuration - secure Kafka

Usually, production environments require some kind of authentication in order to connect to your desired broker.

### Kerberos SASL_SSL

Just like in the non-secure case, we need to specify our broker here as well. In your `application.properties`
or `.yaml`:

```yaml
spring.kafka.bootstrap-servers: my-kafka-broker-host:9092
```

However, instead of setting the protocol to plaintext, we set it to the required `SASL_SSL` mode, and configure the
required settings:

```yaml
spring.kafka.ssl.trust-store-location: file://${TRUST_STORE_LOCATION}
spring.kafka.ssl.trust-store-password: ${TRUST_STORE_PASSWORD}
spring.kafka.security.protocol: SASL_SSL
spring.kafka.consumer.properties.sasl.mechanism: GSSAPI
spring.kafka.consumer.properties.sasl.kerberos.service.name: kafka
spring.kafka.consumer.properties.ssl.endpoint.identification.algorithm:
```

This means we need a trust store (in JKS format) that lets us trust the broker. If you don't have such a truststore nearby, you can easily create a JKS store
out of two classic PEM/CRT files following any guide on the internet.

The above settings require us to specify two new configuration files: a Kerberos `krb5.conf` configuration file, and
a JAAS additional settings file. Out of the two, the latter can be initialized *inside* the application properties,
and we'll do so:

```yaml
spring.kafka.jaas.enabled: true
spring.kafka.jaas.login-module: com.sun.security.auth.module.Krb5LoginModule
spring.kafka.jaas.control-flag: required
spring.kafka.jaas.options.useKeyTab: true
spring.kafka.jaas.options.storeKey: true
spring.kafka.jaas.options.keyTab: ${KAFKA_KEYTAB}
spring.kafka.jaas.options.principal: ${KAFKA_PRINCIPAL}
```

We won't need a key store or a password, as the authentication itself is solved by using a keytab file. Technically,
this is the only private and vulnerable part of the setup, and this keytab file mustn't be committed anywhere in your
repository.

Once we have this, we're done with the properties file, but we still need a `krb5.conf` file.

This configuration needs to be active *before* the Spring context initializes. If possible, this should be done
with a JVM `-D` flag:

```shell
-Djava.security.krb5.conf=/location/krb5.conf
```

If that's not convenient, you can also configure this anywhere before the Spring context is started, which is
either a static initializer, or somewhere in your main method before the application is started:

```java
public static void main(String[] args) {
    System.setProperty("java.security.krb5.conf",
        Optional.ofNullable(System.getenv("KRB5_LOCATION")).orElse("/location/krb5.conf")
    );
    SpringApplication.run(MyApplication.class, args);
}
```

### Reference

To recap:

1. The `application.properties` needs three parts: the broker connection, the Kafka security configuration, and
   the JAAS configuration.
2. You need an external Kerberos `krb5.conf` file, and you need to activate it through a `-D` JVM flag or a
   generic system property before Spring starts.
3. You need a truststore
4. You need a keytab and the name of the principal present in it to authenticate.

If you used the above configuration examples, you may need to set the following environment variables:

```
TRUST_STORE_LOCATION, TRUST_STORE_PASSWORD, KAFKA_KEYTAB, KAFKA_PRINCIPAL, KRB5_LOCATION
```

And with that, you're done, that's all the configuration you need. Onwards to the listener itself.

## The listener itself

Once you set everything, configuring a `@Service` method to be a Kafka listener is quite simple:

```java
@KafkaListener(topics = "whatever_topic_you_need", groupId = "group_id")
public void persistTransaction(String transaction) {
}
```
