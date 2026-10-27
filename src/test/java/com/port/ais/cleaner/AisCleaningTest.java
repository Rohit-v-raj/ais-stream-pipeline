package com.port.ais.cleaner;

import com.port.ais.model.AisPosition;
import com.port.ais.model.VesselPosition;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AisCleaningTest {

    private static final Instant AT = Instant.parse("2020-09-07T10:00:00Z");

    @Test
    void notAvailableCodesBecomeNull() {
        AisPosition raw = new AisPosition(257371000L, 0L, 0, AT, 62.5954, 6.26347, 102.3, 360.0, 511, 15, 1);

        VesselPosition clean = AisCleaning.clean(raw);

        assertThat(clean.sogKnots()).isNull();
        assertThat(clean.cogDegrees()).isNull();
        assertThat(clean.headingDegrees()).isNull();
        assertThat(clean.lengthMetres()).isNull();
        assertThat(clean.imo()).isNull();
        assertThat(clean.mmsi()).isEqualTo("257371000");
    }

    @Test
    void realValuesAreKept() {
        AisPosition raw = new AisPosition(257371000L, 7423926L, 66, AT, 62.5954, 6.26347, 10.7, 48.8, 49, 0, 1);

        VesselPosition clean = AisCleaning.clean(raw);

        assertThat(clean.sogKnots()).isEqualTo(10.7);
        assertThat(clean.cogDegrees()).isEqualTo(48.8);
        assertThat(clean.headingDegrees()).isEqualTo(49);
        assertThat(clean.lengthMetres()).isEqualTo(66);
        assertThat(clean.imo()).isEqualTo(7423926L);
    }

    @Test
    void aPositionNotAvailableIsRejected() {
        AisPosition raw = new AisPosition(257371000L, null, null, AT, 91, 181, 0.0, 0.0, 0, 0, 1);

        assertThatThrownBy(() -> AisCleaning.clean(raw))
                .isInstanceOf(InvalidPositionException.class)
                .hasMessageContaining("no valid position");
    }

    @Test
    void anMmsiThatIsNotNineDigitsIsRejected() {
        AisPosition raw = new AisPosition(1234L, null, null, AT, 62.0, 6.0, 0.0, 0.0, 0, 0, 1);

        assertThatThrownBy(() -> AisCleaning.clean(raw)).isInstanceOf(InvalidPositionException.class);
    }
}
