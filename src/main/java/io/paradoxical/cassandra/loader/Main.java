package io.paradoxical.cassandra.loader;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class Main {

    private static final String DEFAULT_CQL_PATH = "../db/src/main/resources";

    private static final int DEFAULT_PORT = 9042;

    private static final String DEFAULT_REPLICATION_MAP = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();

        Options options = getOptions();

        try {
            CommandLine line = parser.parse(options, args);

            configureLogging(line.hasOption("debug"));

            DbRunnerConfig dbRunnerConfig = getDbRunnerConfig(line);

            if (dbRunnerConfig.getRecreateDatabase()) {
                recreateDatabase(dbRunnerConfig);
            }

            DbScriptsRunner dbScriptsRunner = new DbScriptsRunner(dbRunnerConfig);

            dbScriptsRunner.run();
        }
        catch (ParseException e) {
            System.out.println("Unexpected exception:" + e.getMessage());
            help(options);
        }
    }

    private static void configureLogging(final boolean debug) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(debug ? Level.ALL : Level.INFO);
    }

    private static void recreateDatabase(DbRunnerConfig dbRunnerConfig) {
        Scanner reader = new Scanner(System.in);
        System.out.println("Are you sure you want to recreate the database? This will delete everything from the " + dbRunnerConfig.getKeyspace() + " keyspace. (y/n)");

        if ("y".equalsIgnoreCase(reader.nextLine())) {
            System.out.println("Enter the name of the keyspace you wish to recreate");

            String enteredKeyspace = reader.nextLine();

            if (!dbRunnerConfig.getKeyspace().equals(enteredKeyspace)) {
                System.out.println("Keyspace did not match with keyspace entered in flag");
                System.exit(0);
            }

            dbRunnerConfig.setKeyspace(enteredKeyspace);
        }
        else {
            System.out.println("Database will not be recreated.");
            System.exit(0);
        }
    }

    private static Options getOptions() {
        Options options = new Options();

        options.addOption(Option.builder("ip")
                                .required(true)
                                .hasArg(true)
                                .desc("Cassandra IP Address")
                                .build());

        options.addOption(Option.builder("createKeyspace")
                                .required(false)
                                .desc("Creates the keyspace")
                                .build());

        options.addOption(Option.builder("rm")
                                .longOpt("replicationMap")
                                .required(false)
                                .hasArg(true)
                                .desc("Replication map for use w/ createKeyspace (default = " + DEFAULT_REPLICATION_MAP + ")")
                                .build());

        options.addOption(Option.builder("p")
                                .longOpt("port")
                                .required(false)
                                .hasArg(true)
                                .desc("Cassandra Port (default = " + DEFAULT_PORT + ")")
                                .build());

        options.addOption(Option.builder("u")
                                .longOpt("username")
                                .required(false)
                                .hasArg(true)
                                .desc("Cassandra Username")
                                .build());

        options.addOption(Option.builder("pw")
                                .longOpt("password")
                                .required(false)
                                .hasArg(true)
                                .desc("Cassandra Password")
                                .build());

        options.addOption(Option.builder("k")
                                .longOpt("keyspace")
                                .required(true)
                                .hasArg(true)
                                .desc("Cassandra Keyspace")
                                .build());

        options.addOption(Option.builder()
                                .longOpt("debug")
                                .required(false)
                                .hasArg(false)
                                .desc("Optional debug flag. Spits out all logs")
                                .build());

        options.addOption(Option.builder("f")
                                .longOpt("file-path")
                                .required(false)
                                .hasArg(true)
                                .desc("CQL File Path (default = " + DEFAULT_CQL_PATH + ")")
                                .build());

        /** If this isn't specified, all scripts will be run with a later version number from the current db version **/
        options.addOption(Option.builder("v")
                                .longOpt("upgrade-version")
                                .required(false)
                                .hasArg(true)
                                .desc("Upgrade to Version")
                                .build());

        options.addOption(Option.builder("recreateDatabase")
                                .required(false)
                                .hasArg(false)
                                .desc("Deletes all tables. WARNING all data will be deleted!")
                                .build());

        return options;
    }

    private static DbRunnerConfig getDbRunnerConfig(CommandLine line) {
        DbRunnerConfig.DbRunnerConfigBuilder dbConfigBuilder = DbRunnerConfig.builder();

        String username = line.getOptionValue("u");
        String password = line.getOptionValue("pw");
        String replicationMap = line.hasOption("replicationMap") ? line.getOptionValue("replicationMap") : DEFAULT_REPLICATION_MAP;

        dbConfigBuilder.ip(line.getOptionValue("ip"))
                       .port(line.hasOption("p") ? Integer.valueOf(line.getOptionValue("p")) : DEFAULT_PORT)
                       .username(username != null ? username : "")
                       .password(password != null ? password : "")
                       .createKeyspace(line.hasOption("createKeyspace"))
                       .replicationMap(replicationMap)
                       .keyspace(line.getOptionValue("k"))
                       .dbVersion(line.hasOption("v") ? Integer.valueOf(line.getOptionValue("v")) : null)
                       .filePath(line.hasOption("f") ? line.getOptionValue("f") : DEFAULT_CQL_PATH)
                       .recreateDatabase(line.hasOption("recreateDatabase"));

        return dbConfigBuilder.build();
    }

    private static void help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }
}
