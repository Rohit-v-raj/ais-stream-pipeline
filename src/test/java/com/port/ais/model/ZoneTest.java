package com.port.ais.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ZoneTest {

    private static final Zone FLORO = new Zone("Floro", 61.600, 5.030, 5);
    private static final Zone MALOY = new Zone("Maloy", 61.935, 5.113, 5);

    @Test
    void oneMinuteOfLatitudeIsOneNauticalMile() {
        assertThat(Zone.distanceNm(61.0, 5.0, 61.0 + 1.0 / 60, 5.0)).isCloseTo(1.0, within(0.01));
    }

    @Test
    void floroToMaloyIsAboutTwentyNauticalMiles() {
        assertThat(Zone.distanceNm(FLORO.lat(), FLORO.lon(), MALOY.lat(), MALOY.lon())).isCloseTo(20.3, within(0.5));
    }

    @Test
    void aVesselInsideStaysUntilItIsClearlyOut() {
        List<Zone> zones = List.of(FLORO, MALOY);
        double justOutside = 61.600 + 5.3 / 60;
        assertThat(Zone.locate(zones, null, justOutside, 5.030)).isNull();
        assertThat(Zone.locate(zones, FLORO, justOutside, 5.030)).isEqualTo(FLORO);
        assertThat(Zone.locate(zones, FLORO, 61.600 + 6.0 / 60, 5.030)).isNull();
    }
}
