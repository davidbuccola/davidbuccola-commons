package net.davidbuccola.commons;

import com.google.common.io.ByteStreams;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for working with resources.
 */
public final class ResourceUtils {

    private ResourceUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    @SuppressWarnings("UnstableApiUsage")
    public static String getResourceAsString(String resource) {
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
            if (inputStream == null) {
                throw new FileNotFoundException(resource);
            }
            return new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getResourceLinesAsStrings(String resource) {
        List<String> lines = new ArrayList<>();
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
            if (is == null) {
                throw new FileNotFoundException(resource);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line.trim());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return lines;
    }


}
