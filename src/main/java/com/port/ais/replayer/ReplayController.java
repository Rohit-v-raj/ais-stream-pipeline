package com.port.ais.replayer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Start, stop and pace the replay while it runs. */
@RestController
@RequestMapping("/api/replay")
@ConditionalOnProperty(prefix = "pipeline.replayer", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ReplayController {

    private final Replayer replayer;

    public ReplayController(Replayer replayer) {
        this.replayer = replayer;
    }

    @GetMapping
    public Replayer.Status status() {
        return replayer.status();
    }

    @PostMapping("/start")
    public Replayer.Status start() {
        return replayer.start();
    }

    @PostMapping("/stop")
    public Replayer.Status stop() {
        return replayer.stop();
    }

    /** For example {@code PUT /api/replay/speed?value=200} to make the consumers fall behind. */
    @PutMapping("/speed")
    public Replayer.Status speed(@RequestParam double value) {
        return replayer.changeSpeed(value);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
