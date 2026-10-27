package com.port.ais.replayer;

import com.port.ais.model.AisPosition;

import java.util.Iterator;

/** Position reports in time order: from an exported file, or generated. */
public interface AisSource extends Iterator<AisPosition>, AutoCloseable {

    /** What the status endpoint shows as the source. */
    String describe();

    @Override
    void close();
}
