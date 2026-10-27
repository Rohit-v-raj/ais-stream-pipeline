package com.port.ais.cleaner;

import com.port.ais.TopicNames;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What the cleaner does when a batch fails. Spring Boot gives this error
 * handler to every {@code @KafkaListener}.
 *
 * Two kinds of failure, two answers:
 * <ul>
 *   <li><b>A bad record</b> ({@link InvalidPositionException}): no retry, it will
 *       never become valid. The listener names the record's position in the
 *       batch; the records before it are committed, it goes to the dead-letter
 *       topic, and the consumer carries on after it. One bad record must not
 *       stop a partition. A record that is not JSON at all is caught the same
 *       way by Spring Kafka itself, before the listener runs, and dead-lettered
 *       as its original bytes with a {@code DeserializationException}.</li>
 *   <li><b>Kafka unavailable</b> (writes refused or timing out): the records are
 *       fine, so they are never dead-lettered. The whole batch is retried, after
 *       1 s, 2 s, 4 s ... up to every 30 s, for as long as it takes.</li>
 * </ul>
 */
@Configuration
public class CleanerErrorHandlingConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> jsonTemplate,
                                          ProducerFactory<String, Object> producerFactory) {
        // A record that could not be deserialized is republished as its raw
        // bytes, which the JSON template could not send. Most specific first.
        // Built from Spring Boot's own producer factory, so it uses the same
        // brokers. KafkaProperties alone would miss connection details set
        // elsewhere (Testcontainers does this), and the dead letters would go
        // to whatever cluster application.yml names.
        KafkaTemplate<String, byte[]> bytesTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(), new StringSerializer(), new ByteArraySerializer()));
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, jsonTemplate);

        // Partition -1: let Kafka choose. The default would reuse the failed
        // record's partition number, and the dead-letter topic has only one.
        DeadLetterPublishingRecoverer toDeadLetters = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(TopicNames.DEAD_LETTERS, -1));

        // No maximum elapsed time: retry until Kafka takes the writes again.
        ExponentialBackOff untilKafkaIsBack = new ExponentialBackOff(1000, 2);
        untilKafkaIsBack.setMaxInterval(30_000);

        DefaultErrorHandler handler = new DefaultErrorHandler(toDeadLetters, untilKafkaIsBack);
        handler.addNotRetryableExceptions(InvalidPositionException.class);
        return handler;
    }
}
