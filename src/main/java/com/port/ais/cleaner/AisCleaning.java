package com.port.ais.cleaner;

import com.port.ais.model.AisPosition;
import com.port.ais.model.VesselPosition;

/**
 * Turns a raw report into a clean one. AIS says "not available" with
 * real-looking numbers; here they become null, so no consumer downstream
 * can mistake 102.3 for a speed. A report without a usable position or
 * identity is rejected.
 */
public final class AisCleaning {

    static final double SOG_NOT_AVAILABLE = 102.3;
    static final double COG_NOT_AVAILABLE = 360.0;
    static final int HEADING_NOT_AVAILABLE = 511;

    private AisCleaning() {}

    public static VesselPosition clean(AisPosition raw) {
        if (raw == null) {
            // Spring Kafka dead-letters a record that is not JSON before the listener
            // sees it; this only guards against an empty value (a tombstone).
            throw new InvalidPositionException("Empty value: not a position report");
        }
        if (raw.mmsi() < 100_000_000L || raw.mmsi() > 999_999_999L) {
            throw new InvalidPositionException("MMSI " + raw.mmsi() + " is not nine digits");
        }
        if (raw.timestamp() == null) {
            throw new InvalidPositionException("Report from " + raw.mmsi() + " has no timestamp");
        }
        // 91 and 181 are AIS for "position not available"; anything past ±90 / ±180 is unusable.
        if (Math.abs(raw.lat()) > 90 || Math.abs(raw.lon()) > 180) {
            throw new InvalidPositionException("Report from " + raw.mmsi() + " at " + raw.timestamp()
                    + " has no valid position (" + raw.lat() + ", " + raw.lon() + ")");
        }
        return new VesselPosition(
                String.valueOf(raw.mmsi()),
                positiveOrNull(raw.imo()),
                raw.timestamp(),
                raw.lat(),
                raw.lon(),
                raw.sog() == null || raw.sog() >= SOG_NOT_AVAILABLE || raw.sog() < 0 ? null : raw.sog(),
                raw.cog() == null || raw.cog() >= COG_NOT_AVAILABLE || raw.cog() < 0 ? null : raw.cog(),
                raw.heading() == null || raw.heading() >= 360 || raw.heading() < 0 ? null : raw.heading(),
                raw.navStatus(),
                raw.length() == null || raw.length() <= 0 ? null : raw.length());
    }

    private static Long positiveOrNull(Long value) {
        return value == null || value <= 0 ? null : value;
    }
}
