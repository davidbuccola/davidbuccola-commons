package net.davidbuccola.commons;

import org.apache.commons.cli.*;

/**
 * Utilities to help with commons-cli usage.
 */
@SuppressWarnings("WeakerAccess")
public final class CommandLineUtils {

    private CommandLineUtils() {
        throw new UnsupportedOperationException("Can't be instantiated");
    }

    public static CommandLine parseCommandLine(String syntax, Options options, String[] args) {
        return parseCommandLine(syntax, options, args, false);
    }

    public static CommandLine parseCommandLine(String syntax, Options options, String[] args, boolean stopAtNonOption) {
        try {
            CommandLine command = new DefaultParser().parse(options, args, stopAtNonOption);

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
