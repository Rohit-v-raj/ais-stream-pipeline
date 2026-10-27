package com.port.ais.config;

import com.port.ais.model.Zone;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.util.List;

/** The {@code pipeline.*} settings in {@code application.yml}. */
@ConfigurationProperties("pipeline")
public record PipelineProperties(Topics topics, Replayer replayer, Cleaner cleaner, Streams streams, List<Zone> zones) {

    public record Topics(int partitions, int replicationFactor) {}

    public record Replayer(boolean enabled, boolean autoStart, String source, Path file, double speed,
                           long maxRecords, double badRecordRate, int syntheticVessels) {}

    public record Cleaner(boolean enabled, int concurrency) {}

    public record Streams(boolean enabled) {}
}
