package com.port.ais.streams;

import com.port.ais.config.PipelineProperties;
import com.port.ais.model.VesselPosition;
import com.port.ais.model.Zone;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <b>Interactive queries</b>: the REST API reads Kafka Streams' state stores
 * directly. No database: the "table" of latest positions lives in this
 * process, kept up to date by the stream and backed by a changelog topic.
 *
 * One limit: with several instances, each holds only its own partitions'
 * vessels. A full answer would ask the other instances too (roadmap).
 */
@RestController
@RequestMapping("/api")
@ConditionalOnProperty(prefix = "pipeline.streams", name = "enabled", havingValue = "true", matchIfMissing = true)
public class VesselController {

    public record VesselView(String zone, VesselPosition position) {}

    public record HourlyArrivals(Instant hourStart, long arrivals) {}

    private final StreamsBuilderFactoryBean streams;
    private final List<Zone> zones;

    public VesselController(StreamsBuilderFactoryBean streams, PipelineProperties properties) {
        this.streams = streams;
        this.zones = properties.zones();
    }

    @GetMapping("/vessels/{mmsi}")
    public VesselView vessel(@PathVariable String mmsi) {
        VesselPosition position = latest().get(mmsi);
        if (position == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No position for MMSI " + mmsi + " yet");
        }
        return new VesselView(zoneOf(position), position);
    }

    /** Vessels now inside a port area, or the count of all vessels when no zone is given. */
    @GetMapping("/vessels")
    public Object vessels(@RequestParam(required = false) String zone) {
        ReadOnlyKeyValueStore<String, VesselPosition> store = latest();
        if (zone == null) {
            // Counted, not store.approximateNumEntries(): RocksDB's estimate counts
            // recent updates too and can be off by ten times.
            long count = 0;
            try (KeyValueIterator<String, VesselPosition> all = store.all()) {
                for (; all.hasNext(); all.next()) {
                    count++;
                }
            }
            return Map.of("vessels", count, "zones", zones.stream().map(Zone::name).toList());
        }
        Zone wanted = Zone.named(zones, zone);
        if (wanted == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No port area named " + zone);
        }
        List<VesselView> inside = new ArrayList<>();
        try (KeyValueIterator<String, VesselPosition> all = store.all()) {
            all.forEachRemaining(entry -> {
                if (wanted.contains(entry.value.lat(), entry.value.lon())) {
                    inside.add(new VesselView(wanted.name(), entry.value));
                }
            });
        }
        return inside;
    }

    /** Arrivals per hour of AIS time, from the windowed count. */
    @GetMapping("/ports/{zone}/arrivals")
    public List<HourlyArrivals> arrivals(@PathVariable String zone) {
        ReadOnlyWindowStore<String, Long> store = kafkaStreams().store(StoreQueryParameters.fromNameAndType(
                PositionTopology.TRAFFIC_STORE, QueryableStoreTypes.windowStore()));
        List<HourlyArrivals> hours = new ArrayList<>();
        try (WindowStoreIterator<Long> windows = store.fetch(zone, Instant.EPOCH, Instant.now().plusSeconds(86_400))) {
            while (windows.hasNext()) {
                KeyValue<Long, Long> window = windows.next();
                hours.add(new HourlyArrivals(Instant.ofEpochMilli(window.key), window.value));
            }
        }
        return hours;
    }

    private ReadOnlyKeyValueStore<String, VesselPosition> latest() {
        return kafkaStreams().store(StoreQueryParameters.fromNameAndType(
                PositionTopology.LATEST_STORE, QueryableStoreTypes.keyValueStore()));
    }

    private KafkaStreams kafkaStreams() {
        KafkaStreams kafkaStreams = streams.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka Streams is "
                    + (kafkaStreams == null ? "not started" : kafkaStreams.state()) + "; try again shortly");
        }
        return kafkaStreams;
    }

    private String zoneOf(VesselPosition position) {
        return zones.stream().filter(z -> z.contains(position.lat(), position.lon()))
                .map(Zone::name).findFirst().orElse(null);
    }

    /** Stores move between instances during a rebalance and are briefly unavailable. */
    @ExceptionHandler(InvalidStateStoreException.class)
    public ResponseEntity<Map<String, String>> rebalancing(InvalidStateStoreException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "State store not available (rebalancing?): " + e.getMessage()));
    }
}
