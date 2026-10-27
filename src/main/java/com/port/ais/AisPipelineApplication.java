package com.port.ais;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Three stages in one process, each of which can be switched off to run it on
 * its own: the replayer (producer), the cleaner (consumer group) and the
 * Kafka Streams app. They talk to each other only through Kafka topics.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class AisPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(AisPipelineApplication.class, args);
    }
}
