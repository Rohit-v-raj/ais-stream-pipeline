package com.port.ais.streams;

import com.port.ais.TopicNames;
import com.port.ais.model.VesselPosition;
import com.port.ais.model.Zone;
import com.port.ais.model.ZoneEvent;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The topology on {@link TopologyTestDriver}: a fake Kafka in memory. Records
 * piped in are processed at once, in order, and the outputs and state
 * stores can be read straight back. No broker, no Docker, milliseconds.
 */
class PositionTopologyTest {

    private static final List<Zone> ZONES = List.of(new Zone("Floro", 61.600, 5.030, 5));
    private static final double AT_SEA = 61.80;         // 12 nm north of Florø
    private static final double IN_PORT = 61.600;
    private static final double JUST_OUTSIDE = 61.6883; // 5.3 nm: past the edge, inside the leave margin
    private static final double CLEARLY_OUT = 61.75;    // 9 nm

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, VesselPosition> positions;
    private TestOutputTopic<String, VesselPosition> latest;
    private TestOutputTopic<String, ZoneEvent> zoneEvents;
    private TestOutputTopic<String, Long> traffic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        PositionTopology.build(builder, ZONES);
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AisTimestampExtractor.class);
        driver = new TopologyTestDriver(builder.build(), config);

        positions = driver.createInputTopic(TopicNames.POSITIONS, new StringSerializer(),
                PositionTopology.json(VesselPosition.class).serializer());
        latest = driver.createOutputTopic(TopicNames.VESSEL_LATEST, new StringDeserializer(),
                PositionTopology.json(VesselPosition.class).deserializer());
        zoneEvents = driver.createOutputTopic(TopicNames.ZONE_EVENTS, new StringDeserializer(),
                PositionTopology.json(ZoneEvent.class).deserializer());
        traffic = driver.createOutputTopic(TopicNames.PORT_TRAFFIC, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void theNewestReportIsEachVesselsLatestPosition() {
        send("257000001", "10:00:00", AT_SEA);
        send("257000001", "10:05:00", IN_PORT);
        send("257000001", "10:02:00", AT_SEA);     // late, older: must not replace the newer one
        send("257000002", "10:01:00", AT_SEA);

        KeyValueStore<String, VesselPosition> store = driver.getKeyValueStore(PositionTopology.LATEST_STORE);
        assertThat(store.get("257000001").timestamp()).isEqualTo(at("10:05:00"));
        assertThat(store.get("257000002").timestamp()).isEqualTo(at("10:01:00"));

        Map<String, VesselPosition> topic = latest.readKeyValuesToMap();
        assertThat(topic.get("257000001").lat()).isEqualTo(IN_PORT);
    }

    @Test
    void enteringAndLeavingAPortAreaAreEvents() {
        send("257000001", "10:00:00", AT_SEA);
        send("257000001", "10:30:00", IN_PORT);
        send("257000001", "11:00:00", JUST_OUTSIDE);  // GPS wobble near the edge: still in port
        send("257000001", "11:30:00", IN_PORT);
        send("257000001", "12:00:00", CLEARLY_OUT);

        List<ZoneEvent> events = zoneEvents.readValuesToList();
        assertThat(events).extracting(ZoneEvent::type).containsExactly(ZoneEvent.Type.ENTERED, ZoneEvent.Type.LEFT);
        assertThat(events).extracting(ZoneEvent::at).containsExactly(at("10:30:00"), at("12:00:00"));
        assertThat(events).allSatisfy(e -> assertThat(e.zone()).isEqualTo("Floro"));
    }

    @Test
    void aVesselFirstSeenInPortHasNotArrived() {
        send("257000001", "10:00:00", IN_PORT);
        send("257000001", "10:10:00", IN_PORT);

        assertThat(zoneEvents.isEmpty()).isTrue();
    }

    @Test
    void arrivalsAreCountedPerPortPerHourOfAisTime() {
        send("257000001", "10:00:00", AT_SEA);
        send("257000002", "10:00:00", AT_SEA);
        send("257000001", "10:20:00", IN_PORT);
        send("257000002", "10:40:00", IN_PORT);
        send("257000001", "11:10:00", AT_SEA);       // leaves, then
        send("257000001", "11:30:00", IN_PORT);      // arrives again, in the next hour

        Map<String, Long> counts = traffic.readKeyValuesToMap();
        assertThat(counts).containsEntry("Floro@2020-09-07T10:00:00Z", 2L)
                .containsEntry("Floro@2020-09-07T11:00:00Z", 1L);
    }

    @Test
    void earlierHoursStayQueryableLaterInTheDay() {
        send("257000001", "01:00:00", AT_SEA);
        send("257000001", "01:20:00", IN_PORT);
        send("257000002", "06:30:00", AT_SEA);       // stream time moves on five hours

        WindowStore<String, Long> store = driver.getWindowStore(PositionTopology.TRAFFIC_STORE);
        try (WindowStoreIterator<Long> windows = store.fetch("Floro", at("00:00:00"), at("23:00:00"))) {
            assertThat(windows.hasNext()).isTrue();
            KeyValue<Long, Long> first = windows.next();
            assertThat(Instant.ofEpochMilli(first.key)).isEqualTo(at("01:00:00"));
            assertThat(first.value).isEqualTo(1L);
        }
    }

    private void send(String mmsi, String time, double lat) {
        positions.pipeInput(mmsi, new VesselPosition(mmsi, null, at(time), lat, 5.030, 10.0, 180.0, 180, 0, 100));
    }

    private static Instant at(String time) {
        return Instant.parse("2020-09-07T" + time + "Z");
    }
}
