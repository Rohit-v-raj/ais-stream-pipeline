package com.port.ais.replayer;

import com.port.ais.TopicNames;
import com.port.ais.config.PipelineProperties;
import com.port.ais.model.AisPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The producer. Plays position reports into {@code ais.positions.raw} at
 * {@code speed} times the pace of their own timestamps.
 *
 * The record key is the MMSI. Kafka sends every record with the same key to
 * the same partition, and keeps order only within a partition, so each
 * vessel's reports stay in order however many partitions there are.
 */
@Component
@ConditionalOnProperty(prefix = "pipeline.replayer", name = "enabled", havingValue = "true", matchIfMissing = true)
public class Replayer {

    private static final Logger log = LoggerFactory.getLogger(Replayer.class);

    public record Status(boolean running, String source, double speed, long sent, long acknowledged,
                         long failed, long corrupted, Instant aisTime, String lastError) {}

    private final KafkaTemplate<String, Object> kafka;
    private final PipelineProperties properties;

    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong corrupted = new AtomicLong();
    private volatile boolean running;
    private volatile double speed;
    private volatile boolean speedChanged;
    private volatile Instant aisTime;
    private volatile String source = "not started";
    private volatile String lastError;
    private Thread thread;

    public Replayer(KafkaTemplate<String, Object> kafka, PipelineProperties properties) {
        this.kafka = kafka;
        this.properties = properties;
        this.speed = properties.replayer().speed();
    }

    @EventListener(ApplicationReadyEvent.class)
    void autoStart() {
        if (properties.replayer().autoStart()) {
            start();
        }
    }

    public synchronized Status start() {
        if (!running) {
            running = true;
            thread = Thread.ofPlatform().name("replayer").daemon().start(this::run);
        }
        return status();
    }

    public synchronized Status stop() {
        running = false;
        return status();
    }

    public Status changeSpeed(double newSpeed) {
        if (newSpeed <= 0) {
            throw new IllegalArgumentException("speed must be more than 0");
        }
        speed = newSpeed;
        speedChanged = true;
        return status();
    }

    public Status status() {
        return new Status(running, source, speed, sent.get(), acknowledged.get(), failed.get(),
                corrupted.get(), aisTime, lastError);
    }

    private void run() {
        long max = properties.replayer().maxRecords();
        try (AisSource reports = openSource()) {
            source = reports.describe();
            log.info("Replaying from {} at {}x", source, speed);
            Instant anchorAis = null;
            long anchorNanos = 0;
            while (running && reports.hasNext() && (max <= 0 || sent.get() < max)) {
                AisPosition report = reports.next();
                if (anchorAis == null || speedChanged) {
                    // Pace from here: real time elapsed = AIS time elapsed / speed.
                    anchorAis = report.timestamp();
                    anchorNanos = System.nanoTime();
                    speedChanged = false;
                }
                long dueNanos = anchorNanos + (long) (Duration.between(anchorAis, report.timestamp()).toNanos() / speed);
                long early = dueNanos - System.nanoTime();
                if (early > 2_000_000) {
                    Thread.sleep(Duration.ofNanos(early));
                }
                send(report);
                aisTime = report.timestamp();
            }
            log.info("Replay finished: {} sent, {} acknowledged, {} failed", sent, acknowledged, failed);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            lastError = e.toString();
            log.error("Replay stopped", e);
        } finally {
            running = false;
        }
    }

    private void send(AisPosition report) {
        if (ThreadLocalRandom.current().nextDouble() < properties.replayer().badRecordRate()) {
            report = report.withLat(91);            // AIS: latitude not available
            corrupted.incrementAndGet();
        }
        sent.incrementAndGet();
        // send() returns at once; the producer batches records and sends them in
        // the background. The callback runs when the brokers have acknowledged.
        kafka.send(TopicNames.RAW_POSITIONS, String.valueOf(report.mmsi()), report)
                .whenComplete((result, error) -> {
                    if (error == null) {
                        acknowledged.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                        lastError = error.getMessage();
                    }
                });
    }

    private AisSource openSource() {
        PipelineProperties.Replayer settings = properties.replayer();
        boolean fileExists = settings.file() != null && Files.isRegularFile(settings.file());
        return switch (settings.source()) {
            case "file" -> new FileAisSource(settings.file());
            case "synthetic" -> new SyntheticAisSource(properties.zones(), settings.syntheticVessels());
            case "auto" -> fileExists
                    ? new FileAisSource(settings.file())
                    : new SyntheticAisSource(properties.zones(), settings.syntheticVessels());
            default -> throw new IllegalArgumentException("pipeline.replayer.source must be file, synthetic or auto");
        };
    }
}
