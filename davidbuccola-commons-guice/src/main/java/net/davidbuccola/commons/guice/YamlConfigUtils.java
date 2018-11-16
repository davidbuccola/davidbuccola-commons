package net.davidbuccola.commons.guice;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;
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

/**
 * Utilities to help find and read a YAML configuration (for use with {@link YamlConfigModule}.
 */
@SuppressWarnings("WeakerAccess")
public class YamlConfigUtils {

    private static final Logger log = LoggerFactory.getLogger(YamlConfigUtils.class);

    /**
     * Gets the configuration name from program arguments then reads the configuration and returns a {@link String}
     * representation of its contents. he following places are tried in order:
     * <nl>
     * <li>current working directory</li>
     * <li>classpath</li>
     * </nl>
     */
    public static String getConfigAsString(String[] args) throws IOException {
        return getConfigAsString(getConfigName(args));
    }

    /**
     * Reads a configuration file and returns a {@link String} representation of its contents. The following places are
     * tried in order:
     * <nl>
     * <li>current working directory</li>
     * <li>classpath</li>
     * </nl>
     */
    public static String getConfigAsString(String name) throws IOException {
        return new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                return getConfigAsStream(name);
            }
        }.asCharSource(Charsets.UTF_8).read();
    }

    /**
     * Reads a configuration file and returns an {@link InputStream} representation of its contents. The following
     * places are tried in order:
     * <nl>
     * <li>current working directory</li>
     * <li>classpath</li>
     * </nl>
     */
    public static InputStream getConfigAsStream(String name) throws IOException {
        Path path = Paths.get(name);
        if (Files.isRegularFile(path)) {
            log.info("Reading configuration from local file: " + path.toAbsolutePath());

            return Files.newInputStream(path);
        }

        URL url = Thread.currentThread().getContextClassLoader().getResource(name);
        if (url != null) {
            log.info("Reading configuration from resource: " + url);

            return url.openStream();
        }

        throw new FileNotFoundException("Failed to find configuration file: " + name);
    }

    /**
     * Parses command arguments for a configuration name specified with a "--config" option.
     */
    public static String getConfigName(String[] args) {
        Iterator<String> argCursor = Arrays.asList(args).iterator();
        while (argCursor.hasNext()) {
            String arg = argCursor.next();
            if (arg.startsWith("--config")) {
                String[] pieces = arg.split("=");
                if (pieces.length > 1) {
                    return pieces[1];
                } else {
                    if (argCursor.hasNext()) {
                        String value = argCursor.next();
                        if (!value.startsWith("-")) {
                            return value;
                        }
                    }
                }
            }
        }
        throw new RuntimeException("Missing required argument \"--config=<filename>\"");
    }
}
