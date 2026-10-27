package com.port.ais.config;

import com.port.ais.model.AisPosition;
import com.port.ais.model.Json;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Kafka moves bytes; it does not know about JSON or Java types. These
 * customizers give Spring Boot's producer and consumer factories their
 * serializers: String keys (the MMSI) and plain JSON values.
 *
 * In Java rather than {@code application.yml} so both sides share one
 * {@link Json#MAPPER}, which writes times as readable ISO strings.
 */
@Configuration
public class KafkaSerializationConfig {

    @Bean
    @SuppressWarnings("unchecked")
    DefaultKafkaProducerFactoryCustomizer jsonProducer() {
        return factory -> {
            DefaultKafkaProducerFactory<String, Object> typed = (DefaultKafkaProducerFactory<String, Object>) factory;
            typed.setKeySerializer(new StringSerializer());
            // No type headers: the topic, not a Java class name, says what is in it.
            typed.setValueSerializer(new JsonSerializer<>(Json.MAPPER).noTypeInfo());
        };
    }

    @Bean
    @SuppressWarnings("unchecked")
    DefaultKafkaConsumerFactoryCustomizer jsonConsumer() {
        return factory -> {
            DefaultKafkaConsumerFactory<String, AisPosition> typed = (DefaultKafkaConsumerFactory<String, AisPosition>) factory;
            typed.setKeyDeserializer(new StringDeserializer());
            // A record that is not valid JSON would otherwise fail on every poll,
            // for ever ("poison pill"). The wrapper turns it into an error the
            // error handler can send to the dead-letter topic.
            typed.setValueDeserializer(new ErrorHandlingDeserializer<>(
                    new JsonDeserializer<>(AisPosition.class, Json.MAPPER, false)));
        };
    }
}
