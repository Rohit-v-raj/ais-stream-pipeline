package com.port.ais.replayer;

import com.port.ais.model.AisPosition;
import com.port.ais.model.Zone;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generated ships, for anyone without the AIS export: each sails at 10 to
 * 14 knots to a port area, lies moored there for a few hours, then sails to
 * another. Every vessel reports every 10 seconds of AIS time, never ending.
 * Some reports carry the AIS "not available" codes, as real ones do.
 *
 * Seeded, so every run produces the same ships.
 */
final class SyntheticAisSource implements AisSource {

    private static final Duration REPORT_EVERY = Duration.ofSeconds(10);

    private final List<Zone> zones;
    private final List<Ship> ships = new ArrayList<>();
    private final Random random = new Random(42);
    private Instant tick = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    private int nextShip;

    SyntheticAisSource(List<Zone> zones, int vessels) {
        this.zones = zones;
        for (int i = 0; i < vessels; i++) {
            Zone from = zones.get(i % zones.size());
            Ship ship = new Ship(257_100_000L + i, 90 + random.nextInt(180));
            // Start somewhere between ports, heading for the next one.
            ship.lat = from.lat() + (random.nextDouble() - 0.5) * 0.4;
            ship.lon = from.lon() + (random.nextDouble() - 0.5) * 0.8;
            ship.target = zones.get((i + 1) % zones.size());
            ship.speedKnots = 10 + random.nextDouble() * 4;
            ships.add(ship);
        }
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public AisPosition next() {
        // One report per ship per tick, spread across the tick so times only go forward.
        Ship ship = ships.get(nextShip);
        Instant at = tick.plusMillis(REPORT_EVERY.toMillis() * nextShip / ships.size());
        ship.advance(at);
        AisPosition report = ship.report(at);
        if (++nextShip == ships.size()) {
            nextShip = 0;
            tick = tick.plus(REPORT_EVERY);
        }
        return report;
    }

    @Override
    public String describe() {
        return "synthetic, " + ships.size() + " vessels";
    }

    @Override
    public void close() {
    }

    private final class Ship {
        final long mmsi;
        final int length;
        double lat;
        double lon;
        double speedKnots;
        double course;
        Zone target;
        Instant mooredUntil;
        Instant lastMove;

        Ship(long mmsi, int length) {
            this.mmsi = mmsi;
            this.length = length;
        }

        void advance(Instant now) {
            double hours = lastMove == null ? 0 : Duration.between(lastMove, now).toMillis() / 3_600_000.0;
            lastMove = now;
            if (mooredUntil != null) {
                if (now.isBefore(mooredUntil)) {
                    return;
                }
                mooredUntil = null;
                Zone here = target;
                do {
                    target = zones.get(random.nextInt(zones.size()));
                } while (target == here && zones.size() > 1);
            }
            double distance = Zone.distanceNm(lat, lon, target.lat(), target.lon());
            if (distance < 0.3) {
                mooredUntil = now.plus(Duration.ofMinutes(120 + random.nextInt(360)));
                return;
            }
            double step = Math.min(distance, speedKnots * hours);
            double dLat = (target.lat() - lat);
            double dLon = (target.lon() - lon) * Math.cos(Math.toRadians(lat));
            double norm = Math.hypot(dLat, dLon);
            course = (Math.toDegrees(Math.atan2(dLon, dLat)) + 360) % 360;
            lat += dLat / norm * step / 60;
            lon += dLon / norm * step / 60 / Math.cos(Math.toRadians(lat));
        }

        AisPosition report(Instant at) {
            boolean moored = mooredUntil != null;
            double jitterLat = (random.nextDouble() - 0.5) * 0.0002;
            double jitterLon = (random.nextDouble() - 0.5) * 0.0004;
            Double sog = moored ? 0.0 : Math.round((speedKnots + random.nextGaussian() * 0.3) * 10) / 10.0;
            Double cog = moored ? 360.0 : Math.round(course * 10) / 10.0;          // 360: not available
            Integer heading = random.nextDouble() < 0.05 ? 511 : (int) Math.round(course) % 360; // 511: not available
            if (random.nextDouble() < 0.02) {
                sog = 102.3;                                                        // 102.3: not available
            }
            return new AisPosition(mmsi, 9_000_000L + mmsi % 1_000_000, length, at,
                    lat + jitterLat, lon + jitterLon, sog, cog, heading, moored ? 5 : 0, 1);
        }
    }
}
