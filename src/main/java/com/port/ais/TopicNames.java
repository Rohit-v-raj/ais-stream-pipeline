package com.port.ais;

/**
 * Every topic, in pipeline order. The topics are the pipeline's interface:
 * another service can read any of them without this one knowing.
 */
public final class TopicNames {

    /** Position reports as the replayer read them, "not available" codes and all. Key: MMSI. */
    public static final String RAW_POSITIONS = "ais.positions.raw";

    /** Cleaned position reports. Key: MMSI. */
    public static final String POSITIONS = "ais.positions";

    /** Reports the cleaner could not use, with the reason in the record headers. */
    public static final String DEAD_LETTERS = "ais.positions.dlq";

    /** The latest position of each vessel. Compacted: Kafka keeps the last record per MMSI. */
    public static final String VESSEL_LATEST = "vessel.latest";

    /** A vessel entering or leaving a port area. Key: MMSI. */
    public static final String ZONE_EVENTS = "port.zone-events";

    /** Arrivals per port area per hour. Key: {@code zone@hourStart}. Compacted. */
    public static final String PORT_TRAFFIC = "port.traffic.hourly";

    private TopicNames() {}
}
