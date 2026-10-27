package com.port.ais.streams;

import com.port.ais.model.VesselPosition;
import com.port.ais.model.Zone;
import com.port.ais.model.ZoneEvent;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;
import java.util.Objects;

/**
 * Turns each vessel's stream of positions into ENTERED and LEFT events.
 *
 * It needs memory: "was this ship inside Florø on its last report?". That
 * memory is a <b>state store</b>, a local key-value store (RocksDB on disk)
 * that Kafka Streams also writes to a changelog topic. If this instance dies,
 * whoever takes over its partitions rebuilds the store from the changelog.
 *
 * The first report of a vessel only records where it is. A ship already
 * moored in port when the replay starts has not arrived.
 */
class ZoneTransitionProcessor implements FixedKeyProcessor<String, VesselPosition, ZoneEvent> {

    /** Stored for a vessel outside every port area; a missing key means "never seen". */
    private static final String AT_SEA = "-";

    private final List<Zone> zones;
    private FixedKeyProcessorContext<String, ZoneEvent> context;
    private KeyValueStore<String, String> lastZone;

    ZoneTransitionProcessor(List<Zone> zones) {
        this.zones = zones;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, ZoneEvent> context) {
        this.context = context;
        this.lastZone = context.getStateStore(PositionTopology.ZONE_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, VesselPosition> record) {
        VesselPosition position = record.value();
        if (position == null) {
            return;
        }
        String stored = lastZone.get(record.key());
        Zone before = stored == null || AT_SEA.equals(stored) ? null : Zone.named(zones, stored);
        Zone now = Zone.locate(zones, before, position.lat(), position.lon());
        lastZone.put(record.key(), now == null ? AT_SEA : now.name());

        if (stored == null || Objects.equals(before, now)) {
            return;
        }
        // Same key (MMSI) and same timestamp as the position that caused it.
        if (before != null) {
            context.forward(record.withValue(event(position, before, ZoneEvent.Type.LEFT)));
        }
        if (now != null) {
            context.forward(record.withValue(event(position, now, ZoneEvent.Type.ENTERED)));
        }
    }

    private static ZoneEvent event(VesselPosition position, Zone zone, ZoneEvent.Type type) {
        return new ZoneEvent(position.mmsi(), zone.name(), type, position.timestamp(),
                position.sogKnots(), position.navStatus());
    }
}
