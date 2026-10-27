package com.port.ais.model;

import java.time.Instant;

/** A vessel crossing the edge of a port area, on {@code port.zone-events}. */
public record ZoneEvent(String mmsi, String zone, Type type, Instant at, Double sogKnots, Integer navStatus) {

    public enum Type { ENTERED, LEFT }
}
