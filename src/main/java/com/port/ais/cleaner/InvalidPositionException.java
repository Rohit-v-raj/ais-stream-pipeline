package com.port.ais.cleaner;

/** A report no amount of retrying will fix. It goes straight to the dead-letter topic. */
public class InvalidPositionException extends RuntimeException {

    public InvalidPositionException(String message) {
        super(message);
    }
}
