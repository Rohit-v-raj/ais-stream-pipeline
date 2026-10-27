package com.port.ais.streams;

import com.port.ais.config.PipelineProperties;
import com.port.ais.model.VesselPosition;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Starts Kafka Streams with the {@code spring.kafka.streams.*} settings.
 * Kafka Streams is a library: the app's own threads consume, compute and
 * produce. Its {@code application-id} is also its consumer group, so a
 * second instance with the same id shares the work, partition by partition.
 */
@Configuration
@EnableKafkaStreams
@ConditionalOnProperty(prefix = "pipeline.streams", name = "enabled", havingValue = "true", matchIfMissing = true)
public class StreamsSetup {

    @Bean
    KStream<String, VesselPosition> positionTopology(StreamsBuilder builder, PipelineProperties properties) {
        return PositionTopology.build(builder, properties.zones());
    }
}
