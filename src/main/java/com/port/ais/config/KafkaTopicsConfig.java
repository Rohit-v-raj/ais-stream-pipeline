package com.port.ais.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;

import static com.port.ais.TopicNames.DEAD_LETTERS;
import static com.port.ais.TopicNames.PORT_TRAFFIC;
import static com.port.ais.TopicNames.POSITIONS;
import static com.port.ais.TopicNames.RAW_POSITIONS;
import static com.port.ais.TopicNames.VESSEL_LATEST;
import static com.port.ais.TopicNames.ZONE_EVENTS;

/**
 * The topics, created at start-up by Spring's {@link KafkaAdmin} if they do
 * not exist. The cluster refuses to create topics on first use, so every
 * topic's settings are written down here and nowhere else.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    KafkaAdmin.NewTopics pipelineTopics(PipelineProperties properties) {
        int partitions = properties.topics().partitions();
        int replicas = properties.topics().replicationFactor();
        // A write with acks=all succeeds only while this many replicas are in
        // sync. 2 of 3 survives one broker down; at most the replica count.
        String minInSync = String.valueOf(Math.min(2, replicas));

        return new KafkaAdmin.NewTopics(
                // Partitions decide parallelism: at most one consumer per partition in a group.
                TopicBuilder.name(RAW_POSITIONS).partitions(partitions).replicas(replicas)
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSync)
                        .config(TopicConfig.RETENTION_MS_CONFIG, millis(Duration.ofDays(1)))
                        .build(),
                TopicBuilder.name(POSITIONS).partitions(partitions).replicas(replicas)
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSync)
                        .config(TopicConfig.RETENTION_MS_CONFIG, millis(Duration.ofDays(3)))
                        .build(),
                // One partition: dead letters are few, and one place is easier to inspect.
                TopicBuilder.name(DEAD_LETTERS).partitions(1).replicas(replicas)
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSync)
                        .config(TopicConfig.RETENTION_MS_CONFIG, millis(Duration.ofDays(7)))
                        .build(),
                // Compacted: a table of the latest record per key, kept for ever.
                // Small segments and a low dirty ratio so compaction is quick to see.
                TopicBuilder.name(VESSEL_LATEST).partitions(partitions).replicas(replicas)
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSync)
                        .compact()
                        .config(TopicConfig.SEGMENT_MS_CONFIG, millis(Duration.ofMinutes(1)))
                        .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
                        .build(),
                TopicBuilder.name(ZONE_EVENTS).partitions(partitions).replicas(replicas)
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSync)
                        .build(),
                TopicBuilder.name(PORT_TRAFFIC).partitions(3).replicas(replicas)
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSync)
                        .compact()
                        .build());
    }

    private static String millis(Duration duration) {
        return String.valueOf(duration.toMillis());
    }
}
