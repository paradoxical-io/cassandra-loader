package io.paradoxical.cassandra.loader.db;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.paradoxical.cassandra.loader.DbRunnerConfig;
import io.paradoxical.cassandra.loader.DbScriptsRunner;
import io.paradoxical.cassandra.loader.EmptyCqlDataSet;
import org.apache.cassandra.io.util.FileUtils;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

class LocalCql extends CassandraCQLUnit {
    public static final String custom_file = "/pre-test-cassandra.yml";

    public static final String tmp_folder = "target/test-cassandra";

    private static int clientPort = new Random().nextInt(1000) + 30000;

    public LocalCql() {
        super(null);
    }

    public void startDb() throws Exception {
        prepConfigFile();

        Exception exception = null;

        for (int retries = 0; retries < 3; retries++) {

            try {
                EmbeddedCassandraServerHelper.startEmbeddedCassandra(new File(tmp_folder + custom_file), "target/embeddedCassandra", 10000);

                this.load();

                return;
            }
            catch (Exception ex) {
                exception = ex;
            }
        }

        throw exception;
    }

    @Override protected void load() {
        cluster = new Cluster.Builder().addContactPoints("127.0.0.1").withPort(clientPort).build();
        session = cluster.connect();
        CQLDataLoader dataLoader = new CQLDataLoader(session);
        dataLoader.load(new EmptyCqlDataSet(null, true, true));
        session = dataLoader.getSession();
    }

    private void prepConfigFile() throws IOException {
        rmdir(tmp_folder);

        mkdir(tmp_folder);

        copy(custom_file, tmp_folder);

        File file = new File(tmp_folder + custom_file);

        String s = org.apache.commons.io.FileUtils.readFileToString(file);

        s = randomPort(s, "--STORAGE-PORT--");

        s = randomPort(s, "--THRIFT-PORT--");

        s = setPort(s, "--CLIENT-PORT--", clientPort);

        org.apache.commons.io.FileUtils.writeStringToFile(file, s);
    }

    private static void rmdir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists()) {
            FileUtils.deleteRecursive(new File(dir));
        }
    }

    private static String randomPort(String source, String key) {
        return setPort(source, key, new Random().nextInt(1000) + 30000);
    }

    private static String setPort(String source, String key, int port) {
        return source.replace(key, String.valueOf(port));
    }


    /**
     * Creates a directory
     *
     * @param dir
     * @throws IOException
     */
    private static void mkdir(String dir) throws IOException {
        FileUtils.createDirectory(dir);
    }

    private static void copy(String resource, String directory) throws IOException {
        mkdir(directory);
        String fileName = resource.substring(resource.lastIndexOf("/") + 1);
        File file = new File(directory + System.getProperty("file.separator") + fileName);
        try (
                InputStream is = CqlUnitDb.class.getResourceAsStream(resource);
                OutputStream out = new FileOutputStream(file)
        ) {
            byte buf[] = new byte[1024];
            int len;
            while ((len = is.read(buf)) > 0) {
                out.write(buf, 0, len);
            }
            out.close();
        }
    }
}

public class CqlUnitDb {

    private static Session session;

    private static final Object sync = new Object();

    public static Session create(String filePath) throws Exception {

        if (session == null) {
            synchronized (sync) {
                if (session == null) {

                    LocalCql db = new LocalCql();

                    db.startDb();

                    DbRunnerConfig dbRunnerConfig = DbRunnerConfig.builder()
                                                                  .filePath(filePath)
                                                                  .recreateDatabase(true)
                                                                  .build();

                    DbScriptsRunner dbScriptsRunner = new DbScriptsRunner(dbRunnerConfig);

                    dbScriptsRunner.run(db.session);

                    session = db.session;

                }
            }
        }

        return session;
    }

    public static Session reset(String filePath) throws Exception {
        synchronized (sync) {
            session = null;
        }

        return create(filePath);
    }
}
