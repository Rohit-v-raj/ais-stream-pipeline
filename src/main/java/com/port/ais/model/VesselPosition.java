package com.port.ais.model;

import java.time.Instant;

/**
 * A cleaned position report, on {@code ais.positions} and
 * {@code vessel.latest}. A value AIS reported as "not available" is null.
 */
public record VesselPosition(
        String mmsi,
        Long imo,
        Instant timestamp,
        double lat,
        double lon,
        Double sogKnots,
        Double cogDegrees,
        Integer headingDegrees,
        /** ITU-R M.1371 code: 0 under way, 1 at anchor, 5 moored, 15 undefined. */
        Integer navStatus,
        Integer lengthMetres) {
}
