package com.port.ais.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.kafka.support.JacksonUtils;

/** The one JSON mapper for every topic: Java time support, ISO timestamps, unknown fields ignored. */
public final class Json {

    public static final ObjectMapper MAPPER = JacksonUtils.enhancedObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private Json() {}
}
