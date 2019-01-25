package net.davidbuccola.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

/**
 * Utilities for loading properties from a configuration file and/or inline in the program args.
 */
public class PropertiesUtils {

    private static final Logger log = LoggerFactory.getLogger(PropertiesUtils.class);

    private PropertiesUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static Properties getProperties(String[] args) {
        return getPropertiesPath(args)
            .map(PropertiesUtils::getFileProperties)
            .orElseGet(() -> getInlineProperties(args));
    }

    private static Optional<String> getPropertiesPath(String[] args) {
        Iterator<String> argCursor = Arrays.asList(args).iterator();
        while (argCursor.hasNext()) {
            String arg = argCursor.next();
            if (arg.startsWith("--config")) {
                String[] pieces = arg.split("=");
                if (pieces.length > 1) {
                    return Optional.of(pieces[1]);
                } else {
                    if (argCursor.hasNext()) {
                        String value = argCursor.next();
                        if (!value.startsWith("-")) {
                            return Optional.of(value);
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static Properties getFileProperties(String path) {
        try {
            Properties properties = new Properties();
            properties.load(getPropertiesInputStream(path));
            return properties;

        } catch (IOException e) {
            throw new RuntimeException(e); // Convert to runtime to get through lambdas
        }
    }

    private static InputStream getPropertiesInputStream(String path) throws IOException {
        Path filePath = Paths.get(path);
        if (Files.isRegularFile(filePath)) {
            log.info("Reading configuration from local file: " + filePath.toAbsolutePath());

            return Files.newInputStream(filePath);
        }

        URL url = Thread.currentThread().getContextClassLoader().getResource(path);
        if (url != null) {
            log.info("Reading configuration from resource: " + url);

            return url.openStream();
        }

        throw new FileNotFoundException("Failed to find configuration file: " + path);
    }

    private static Properties getInlineProperties(String[] args) {
        Properties properties = new Properties();
        for (String arg : args) {
            if (arg.length() > 0 && !arg.startsWith("-") && arg.contains("=")) {
                String[] pieces = arg.split("=");
                properties.put(pieces[0], pieces[1]);
            }
        }
        return properties;
    }
}

