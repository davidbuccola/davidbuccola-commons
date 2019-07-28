package net.davidbuccola.commons;

import org.apache.commons.cli.*;

/**
 * Utilities to help with commons-cli usage.
 */
public class CommandLineUtils {

    private CommandLineUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static CommandLine parseCommandLine(String syntax, Options options, String[] args) {
        try {
            CommandLine command = new DefaultParser().parse(options, args);

            if (command.hasOption("help")) {
                new HelpFormatter().printHelp(80, syntax, "\nOptions:", options, null, true);
                System.exit(0);
            }
            return command;

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.flush();
            new HelpFormatter().printHelp(80, syntax, "\nOptions:", options, null, true);
            System.exit(1);
            return null;
        }
    }
}
