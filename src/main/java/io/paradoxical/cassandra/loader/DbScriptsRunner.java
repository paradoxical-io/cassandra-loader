package io.paradoxical.cassandra.loader;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.godaddy.logging.Logger;
import com.godaddy.logging.LoggerFactory;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.FileCQLDataSet;

import java.io.File;
import java.util.*;

import static java.util.stream.Collectors.toList;


public class DbScriptsRunner {

    private static final Logger logger = LoggerFactory.getLogger(DbScriptsRunner.class);


    private final DbRunnerConfig dbRunnerConfig;

    public DbScriptsRunner(DbRunnerConfig dbRunnerConfig) {
        this.dbRunnerConfig = dbRunnerConfig;
    }

    public void run() throws Exception {
        try (Cluster cluster = createCluster(dbRunnerConfig)) {
            try (Session session = cluster.connect()) {
                run(session);
            }
        }
    }

    public void run(Session rootSession) throws Exception {

        String keyspace = getKeyspace(rootSession);

        if (dbRunnerConfig.getCreateKeyspace() != null && dbRunnerConfig.getCreateKeyspace()) {
            try {
                rootSession.execute("CREATE KEYSPACE IF NOT EXISTS " + dbRunnerConfig.getKeyspace() + "\n" +
                                    "WITH REPLICATION = " + dbRunnerConfig.getReplicationMap() +";");
            } catch (AlreadyExistsException ex) {
                logger.warn("Keyspace already exists!");
            }
        }

        try (Session keyspaceSession = rootSession.getCluster().connect(keyspace)) {

            if (dbRunnerConfig.getRecreateDatabase()) {
                dropTables(keyspaceSession, keyspace);
            }

            keyspaceSession.execute("CREATE TABLE IF NOT EXISTS db_version ( " +
                                    "    version int PRIMARY KEY, " +
                                    "    updated_date timestamp " +
                                    ")");

            Integer curDbVersion = getLatestDBVersion(keyspaceSession);

            List<String> files = getFiles(curDbVersion, dbRunnerConfig.getDbVersion(), dbRunnerConfig.getFilePath());

            CQLDataLoader dataLoader = new CQLDataLoader(keyspaceSession);

            for (String file : files) {
                logger.info("Running " + dbRunnerConfig.getFilePath() + file);
                dataLoader.load(new FileCQLDataSet(dbRunnerConfig.getFilePath() + file, false, false));
                logger.info("Completed Running " + dbRunnerConfig.getFilePath() + file);

                /** Note: This is assuming that there is only 1 cql file per db version. **/
                keyspaceSession.execute("insert into db_version(version, updated_date) values(" + Integer.parseInt(file.substring(0, file.indexOf('_'))) +
                        ", dateof(now()))");
            }
        }
    }

    public String getKeyspace(Session rootSession) {
        if (dbRunnerConfig.getKeyspace() != null) {
            return dbRunnerConfig.getKeyspace();
        }

        CQLDataLoader dataLoader = new CQLDataLoader(rootSession);

        return dataLoader.getSession().getLoggedKeyspace();
    }

    private void dropTables(Session keyspaceSession, String keyspace) {
        logger.info("Dropping all tables from keyspace = " + keyspace + ".");
        List<Row> tables = keyspaceSession.execute("SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = '" + keyspace + "';").all();

        for (Row row : tables) {
            logger.info("Dropping table " + row.getString("columnfamily_name") + ".");
            keyspaceSession.execute("DROP TABLE " + row.getString("columnfamily_name"));
        }
    }

    private Cluster createCluster(DbRunnerConfig dbRunnerConfig) {
        final Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(dbRunnerConfig.getIp());
        //builder.withPort(dbRunnerConfig.getPort());
        builder.withAuthProvider(new PlainTextAuthProvider(dbRunnerConfig.getUsername(), dbRunnerConfig.getPassword()));

        return builder.build();
    }

    private List<String> getFiles(Integer curDbVersion, Integer dbVersion, String filePath) throws Exception {
        logger.info("Loading cql scripts from " + filePath);

        File[] listOfFiles = new File(filePath).listFiles();

        if (listOfFiles == null) {
            throw new Exception("No CQL files found for the path Files Found for the file path: " + filePath);
        }

        List<String> fileNames = new ArrayList<>();

        for (final File listOfFile : listOfFiles) {
            if (listOfFile.isFile() && fileWithinVersion(listOfFile.getName(), curDbVersion, dbVersion) && listOfFile.getName().endsWith(".cql")) {
                fileNames.add(listOfFile.getName());
            }
        }

        /** Sort the list so that version 1 files are at the beginning. **/
        Collections.sort(fileNames);

        return fileNames;
    }

    private Boolean fileWithinVersion(String fileName, Integer curDbVersion, Integer dbVersion) {

        if (!fileName.endsWith(".cql")) {
            logger.info("Skipping file: " + fileName);

            return false;
        }

        Integer fileVersion = Integer.parseInt(fileName.substring(0, fileName.indexOf('_')));

        if (dbVersion == null) {
            return fileVersion > curDbVersion;
        }

        return fileVersion <= dbVersion && fileVersion > curDbVersion;
    }

    private Integer getLatestDBVersion(Session session)
    {
        List<Row> rows = session.execute("select version, updated_date from db_version").all();

        if (rows.isEmpty()) {
            return 0;
        }

        Comparator<Row> rowComparator = (row1, row2) -> {
            final Date updated_date = row1.getTimestamp("updated_date");
            final Date updated_date2 = row2.getTimestamp("updated_date");

            return updated_date.compareTo(updated_date2);
        };

        List<Row> rowsSorted = rows.stream().sorted(rowComparator).collect(toList());

        /** Note: The last row from the above list will always be the most recently entered value into the db_version table **/
        return rowsSorted.get(rowsSorted.size() - 1).getInt("version");
    }
}
