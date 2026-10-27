package com.port.ais.streams;

import com.port.ais.model.Json;
import com.port.ais.model.VesselPosition;
import com.port.ais.model.Zone;
import com.port.ais.model.ZoneEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.port.ais.TopicNames.PORT_TRAFFIC;
import static com.port.ais.TopicNames.POSITIONS;
import static com.port.ais.TopicNames.VESSEL_LATEST;
import static com.port.ais.TopicNames.ZONE_EVENTS;

/**
 * The Kafka Streams topology: what happens to every cleaned position.
 *
 * <pre>
 * ais.positions ──► latest per vessel (table) ─────────────► vessel.latest (compacted)
 *              └──► zone transitions (state store) ──┬──────► port.zone-events
 *                                                    └─ ENTERED, re-keyed by zone ──► hourly count ──► port.traffic.hourly
 * </pre>
 *
 * Static and free of Spring, so a test can build it on a fake Kafka
 * ({@code TopologyTestDriver}).
 */
public final class PositionTopology {

    /** Queryable stores; the REST API reads them directly. */
    public static final String LATEST_STORE = "vessel-latest-store";
    public static final String TRAFFIC_STORE = "port-traffic-store";
    static final String ZONE_STORE = "vessel-zone-store";

    private PositionTopology() {}

    public static KStream<String, VesselPosition> build(StreamsBuilder builder, List<Zone> zones) {
        Serde<VesselPosition> positionSerde = json(VesselPosition.class);
        Serde<ZoneEvent> eventSerde = json(ZoneEvent.class);

        KStream<String, VesselPosition> positions =
                builder.stream(POSITIONS, Consumed.with(Serdes.String(), positionSerde));

        // 1. A stream becomes a table: one row per key, the latest value wins.
        //    The newer report wins even if an older one arrives late.
        KTable<String, VesselPosition> latest = positions
                .groupByKey(Grouped.with(Serdes.String(), positionSerde))
                .reduce((current, next) -> next.timestamp().isBefore(current.timestamp()) ? current : next,
                        Materialized.<String, VesselPosition, KeyValueStore<Bytes, byte[]>>as(LATEST_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(positionSerde));
        latest.toStream().to(VESSEL_LATEST, Produced.with(Serdes.String(), positionSerde));

        // 2. Entering and leaving port areas: needs the vessel's previous zone, kept in a store.
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ZONE_STORE), Serdes.String(), Serdes.String()));
        KStream<String, ZoneEvent> zoneEvents =
                positions.processValues(() -> new ZoneTransitionProcessor(zones), ZONE_STORE);
        zoneEvents.to(ZONE_EVENTS, Produced.with(Serdes.String(), eventSerde));

        // 3. Arrivals per port per hour. Counting by zone means re-keying by
        //    zone, and Kafka Streams writes the re-keyed records through an
        //    internal "repartition" topic so each zone's arrivals meet in one place.
        //    Grace: an arrival up to 10 minutes late still counts in its hour.
        //    Retention: by default a window store keeps only size + grace (70
        //    minutes of stream time), so the API could show just the last hour.
        zoneEvents
                .filter((mmsi, event) -> event.type() == ZoneEvent.Type.ENTERED)
                .groupBy((mmsi, event) -> event.zone(), Grouped.with("arrivals-by-zone", Serdes.String(), eventSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(10)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(TRAFFIC_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withRetention(Duration.ofDays(2)))
                .toStream((window, count) -> window.key() + "@" + Instant.ofEpochMilli(window.window().start()))
                .to(PORT_TRAFFIC, Produced.with(Serdes.String(), Serdes.Long()));

        return positions;
    }

    /** Plain JSON, no type headers, the same mapper as the producers. */
    static <T> Serde<T> json(Class<T> type) {
        return new JsonSerde<>(type, Json.MAPPER).noTypeInfo().ignoreTypeHeaders();
    }
}
