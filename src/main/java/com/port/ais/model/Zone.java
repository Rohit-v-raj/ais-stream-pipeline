package com.port.ais.model;

import java.util.List;

/** A port area: a circle of {@code radiusNm} nautical miles around a point. */
public record Zone(String name, double lat, double lon, double radiusNm) {

    /**
     * How far past the edge a vessel already inside must go before it counts
     * as having left. Without it, a ship lying near the edge would "enter"
     * and "leave" with every wobble of its GPS.
     */
    static final double LEAVE_MARGIN_NM = 0.5;

    private static final double EARTH_RADIUS_NM = 3440.065;

    public boolean contains(double atLat, double atLon) {
        return distanceNm(lat, lon, atLat, atLon) <= radiusNm;
    }

    /**
     * The zone a vessel is in now, given the one it was in before (null if
     * none): it stays in its previous zone until it is clearly out of it.
     */
    public static Zone locate(List<Zone> zones, Zone previous, double atLat, double atLon) {
        if (previous != null && distanceNm(previous.lat, previous.lon, atLat, atLon) <= previous.radiusNm + LEAVE_MARGIN_NM) {
            return previous;
        }
        return zones.stream().filter(z -> z.contains(atLat, atLon)).findFirst().orElse(null);
    }

    public static Zone named(List<Zone> zones, String name) {
        return name == null ? null : zones.stream().filter(z -> z.name.equals(name)).findFirst().orElse(null);
    }

    /** Great-circle distance (haversine). */
    public static double distanceNm(double lat1, double lon1, double lat2, double lon2) {
        double p = Math.PI / 180;
        double a = Math.pow(Math.sin((lat2 - lat1) * p / 2), 2)
                + Math.cos(lat1 * p) * Math.cos(lat2 * p) * Math.pow(Math.sin((lon2 - lon1) * p / 2), 2);
        return 2 * EARTH_RADIUS_NM * Math.asin(Math.sqrt(a));
    }
}
