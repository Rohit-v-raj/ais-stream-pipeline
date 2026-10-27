package com.port.ais.replayer;

import com.port.ais.model.AisPosition;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

/**
 * Reads the CSV written by {@code tools/export_ais.py}: real AIS reports,
 * already sorted by time. Columns are found by their header names.
 */
final class FileAisSource implements AisSource {

    private final Path file;
    private final BufferedReader reader;
    private final Map<String, Integer> column = new HashMap<>();
    private String nextLine;

    FileAisSource(Path file) {
        this.file = file;
        try {
            InputStream in = Files.newInputStream(file);
            if (file.toString().endsWith(".gz")) {
                in = new GZIPInputStream(in, 1 << 16);
            }
            reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16);
            String[] header = reader.readLine().split(",");
            for (int i = 0; i < header.length; i++) {
                column.put(header[i].trim(), i);
            }
            nextLine = reader.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextLine != null;
    }

    @Override
    public AisPosition next() {
        if (nextLine == null) {
            throw new NoSuchElementException();
        }
        String[] f = nextLine.split(",", -1);
        try {
            nextLine = reader.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new AisPosition(
                Long.parseLong(f[column.get("mmsi")]),
                optionalLong(f[column.get("imo_nr")]),
                optionalInt(f[column.get("length")]),
                Instant.parse(f[column.get("date_time_utc")]),
                Double.parseDouble(f[column.get("lat")]),
                Double.parseDouble(f[column.get("lon")]),
                optionalDouble(f[column.get("sog")]),
                optionalDouble(f[column.get("cog")]),
                optionalInt(f[column.get("true_heading")]),
                optionalInt(f[column.get("nav_status")]),
                optionalInt(f[column.get("message_nr")]));
    }

    @Override
    public String describe() {
        return "file " + file;
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    private static Long optionalLong(String s) {
        return s.isEmpty() ? null : Long.valueOf(s);
    }

    private static Integer optionalInt(String s) {
        return s.isEmpty() ? null : Integer.valueOf(s);
    }

    private static Double optionalDouble(String s) {
        return s.isEmpty() ? null : Double.valueOf(s);
    }
}
