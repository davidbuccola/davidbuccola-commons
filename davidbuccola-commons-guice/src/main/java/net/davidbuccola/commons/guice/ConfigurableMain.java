package net.davidbuccola.commons.guice;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;

/**
 * A base for a main class that can be configured with Guice and configuration from a YAML file using the facilities of
 * {@link org.gwizard.config}. An important feature of this configurable YAML support is that it is serializable and
 * therefore usable in distributed processing frameworks like Spark.
 * <p>
 * The derived class provides the {@link #main(String[])} entry point and calls {@link #bootstrap(String[], Class,
 * Class, SerializableSupplier)} to activate the configuration support contained herein. In this base class is an
 * example {@link #main(String[])} which shows how the derived class would do this.
 * <p>
 * Command-line use of main classes extending from this is:
 * <pre>
 *     command-name --config=config-file.yml
 * </pre>
 * <p>
 * The format of the YAML config file is discussed with {@link org.gwizard.config}.
 */
public abstract class ConfigurableMain implements Runnable, Serializable {

    private static final String SYNTAX = "<command>";

    private static final Logger log = LoggerFactory.getLogger(ConfigurableMain.class);

    /**
     * An example of what main should look like in the derived class. The derived class should have their own version of
     * this with appropriate arguments to {@link #bootstrap}
     */
    public static void main(String[] args) throws Exception {
        bootstrap(args, ConfigurableMain.class, Object.class, Collections::emptyList);
    }

    /**
     * Main entry to the facilities of this class. The {@link #main} of the derived class should delegate to here.
     */
    protected static <M extends ConfigurableMain, C> void bootstrap(String[] args, Class<M> mainClass, Class<C> configClass, SerializableSupplier<Collection<Module>> modules) throws Exception {
        CommandLine command = parseCommandLine(args);

        String configString = getConfigAsString(command.getOptionValue("config"));
        Injector injector = new LazyInjector() {
            @Override
            protected Collection<Module> getModules() {
                return ImmutableSet.of(
                        new MainModule<>(configString, configClass),
                        Modules.combine(modules.get()));
            }
        };
        injector.getInstance(mainClass).run(); // Transition to instance to get injection
    }

    private static CommandLine parseCommandLine(String[] args) {
        CommandLine commandLine = null;
        Options options = new Options();
        options.addOption(Option.builder().longOpt("help").desc("Print this message").build());
        options.addOption(Option.builder().longOpt("config").hasArg().argName("file path").desc("YAML Config File").required().build());
        try {

            commandLine = new DefaultParser().parse(options, args);

            if (commandLine.hasOption("help")) {
                new HelpFormatter().printHelp(80, ConfigurableMain.SYNTAX, null, options, null, true);
                System.exit(0);
            }
        } catch (ParseException e) {
            new HelpFormatter().printHelp(80, ConfigurableMain.SYNTAX, e.getMessage(), options, null, true);
            System.exit(1);
        }
        return commandLine;
    }

    private static String getConfigAsString(String filename) throws IOException {
        Path configPath = Paths.get(filename).toAbsolutePath();

        log.info("Reading configuration, configPath=" + configPath);

        return new String(Files.readAllBytes(configPath), StandardCharsets.UTF_8);
    }

    /**
     * A {@link Module} which supports {@link com.google.inject} configuration from a YAML file using the facilities of
     * {@link org.gwizard.config}.
     */
    private static final class MainModule<C> extends StringConfigModule {

        private MainModule(String configString, Class<C> configClass) {
            super(configString, configClass);
        }
    }
}
