package com.port.ais;

import com.port.ais.replayer.Replayer;
import com.port.ais.streams.VesselController;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The whole pipeline against a real broker in Docker (Testcontainers):
 * replayer, cleaner, dead-letter topic and Kafka Streams. One broker, so
 * every topic has one replica. Skipped when Docker is not running.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
        "pipeline.topics.replication-factor=1",
        "spring.kafka.streams.replication-factor=1",
        "spring.kafka.streams.application-id=ais-streams-it-${random.uuid}",
        "spring.kafka.streams.state-dir=build/streams-it",
        "pipeline.replayer.source=synthetic",
        "pipeline.replayer.synthetic-vessels=40",
        "pipeline.replayer.speed=1000000",
        "pipeline.replayer.max-records=2000",
        "pipeline.replayer.bad-record-rate=0.05"
})
class PipelineIntegrationTest {

    private static final byte[] POISON = "this is not JSON".getBytes(StandardCharsets.UTF_8);

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.2");

    @Autowired
    Replayer replayer;

    @Autowired
    VesselController vessels;

    @Test
    void reportsFlowFromTheReplayerToTheVesselTableAndBadOnesToDeadLetters() throws Exception {
        // throws: the poison pill's send is awaited, and a failed send must fail the test.
        await().atMost(Duration.ofSeconds(60))
                .until(() -> replayer.status().acknowledged() == 2000);
        long corrupted = replayer.status().corrupted();
        assertThat(corrupted).isPositive();

        // A "poison pill": bytes that are not JSON at all.
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(TopicNames.RAW_POSITIONS, "257999999", POISON)).get(10, TimeUnit.SECONDS);
        }

        // Every one of the 40 generated vessels reaches the Kafka Streams table.
        await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted(() ->
                LongStream.range(0, 40).forEach(i -> assertThat(vessels.vessel(String.valueOf(257_100_000L + i))).isNotNull()));

        // Every corrupted report ends up as a dead letter, with the reason in a header.
        List<ConsumerRecord<String, byte[]>> deadLetters = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlq-check",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(TopicNames.DEAD_LETTERS));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (deadLetters.size() < corrupted + 1 && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(deadLetters::add);
            }
        }
        assertThat(deadLetters).filteredOn(r -> header(r, "kafka_dlt-exception-message").contains("no valid position"))
                .hasSize((int) corrupted);

        // The poison pill keeps its original bytes. Spring Kafka caught it while
        // deserializing, before the listener ran, and says so.
        List<ConsumerRecord<String, byte[]>> pills = deadLetters.stream()
                .filter(r -> !header(r, "kafka_dlt-exception-message").contains("no valid position")).toList();
        assertThat(pills).extracting(r -> r.key() + " / " + header(r, "kafka_dlt-exception-fqcn"))
                .containsExactly("257999999 / org.springframework.kafka.support.serializer.DeserializationException");
        assertThat(pills.get(0).value()).isEqualTo(POISON);
        assertThat(header(pills.get(0), "kafka_dlt-exception-fqcn")).endsWith("DeserializationException");
        assertThat(deadLetters).hasSize((int) corrupted + 1);
    }

    private static String header(ConsumerRecord<String, byte[]> deadLetter, String name) {
        Header header = deadLetter.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
