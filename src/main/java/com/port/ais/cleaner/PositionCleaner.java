package com.port.ais.cleaner;

import com.port.ais.TopicNames;
import com.port.ais.model.AisPosition;
import com.port.ais.model.VesselPosition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The consumer. Every instance of this app joins the consumer group
 * {@code ais-cleaner}, and Kafka shares the topic's partitions out among all
 * the group's consumers: start a second instance and each gets half.
 *
 * It takes a whole poll (up to 500 records) at a time, sends every cleaned
 * record without waiting, then waits once for all of them. Waiting for each
 * record in turn costs a broker round trip per record (about 10 ms with
 * {@code acks=all}), which capped one consumer at roughly 100 records a second.
 *
 * Delivery is <b>at least once</b>. The container commits the batch's offsets
 * only after this method returns, and it returns only when every record is
 * acknowledged. After a crash, or a retried batch, some records are cleaned
 * and sent again. Cleaning the same report twice gives the same result, so a
 * repeat does no harm downstream.
 */
@Component
@ConditionalOnProperty(prefix = "pipeline.cleaner", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PositionCleaner {

    private final KafkaTemplate<String, Object> kafka;

    public PositionCleaner(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    /**
     * {@code concurrency}: consumer threads in this instance, each with its own share of partitions.
     * {@code idIsGroup = false}: otherwise Spring would use the listener id as the group id,
     * instead of {@code spring.kafka.consumer.group-id} ({@code ais-cleaner}).
     */
    @KafkaListener(id = "cleaner", idIsGroup = false, topics = TopicNames.RAW_POSITIONS, batch = "true",
            concurrency = "${pipeline.cleaner.concurrency:3}")
    public void clean(List<ConsumerRecord<String, AisPosition>> records) {
        List<CompletableFuture<SendResult<String, Object>>> sent = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, AisPosition> record = records.get(i);
            VesselPosition clean;
            try {
                clean = AisCleaning.clean(record.value());
            } catch (InvalidPositionException e) {
                // The error handler will commit the offsets before record i and
                // dead-letter record i, so the records before it must be written first.
                awaitAll(sent);
                throw new BatchListenerFailedException(e.getMessage(), e, i);
            }
            sent.add(kafka.send(TopicNames.POSITIONS, record.key(), clean));
        }
        awaitAll(sent);
    }

    private static void awaitAll(List<CompletableFuture<SendResult<String, Object>>> sent) {
        try {
            CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while forwarding cleaned reports", e);
        } catch (ExecutionException | TimeoutException e) {
            // Kafka could not take the writes (brokers down, say). Not the records'
            // fault: the error handler retries the batch until Kafka is back.
            throw new IllegalStateException("Could not forward cleaned reports: " + e.getMessage(), e);
        }
    }
}
