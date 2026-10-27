package com.port.ais.model;

import java.time.Instant;

/**
 * A position report exactly as received, on {@code ais.positions.raw}. AIS
 * codes "not available" as real-looking numbers (speed 102.3, course 360,
 * heading 511, latitude 91, longitude 181, length 0); they are still here.
 */
public record AisPosition(
        long mmsi,
        Long imo,
        Integer length,
        Instant timestamp,
        double lat,
        double lon,
        Double sog,
        Double cog,
        Integer heading,
        Integer navStatus,
        Integer messageType) {

    public AisPosition withLat(double newLat) {
        return new AisPosition(mmsi, imo, length, timestamp, newLat, lon, sog, cog, heading, navStatus, messageType);
    }
}
