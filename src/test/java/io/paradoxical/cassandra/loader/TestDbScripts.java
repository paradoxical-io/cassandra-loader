package io.paradoxical.cassandra.loader;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.Lists;
import org.cassandraunit.CassandraCQLUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TestDbScripts {

    @Rule
    public CassandraCQLUnit emptyDb = new CassandraCQLUnit(new EmptyCqlDataSet(null, true, false));

    @Test
    public void test_runner() throws Exception {

        DbRunnerConfig dbRunnerConfig = DbRunnerConfig.builder()
                                                      .dbVersion(1)
                                                      .filePath("src/test/resources")
                                                      .recreateDatabase(false)
                                                      .build();

        DbScriptsRunner dbScriptsRunner = new DbScriptsRunner(dbRunnerConfig);

        dbScriptsRunner.run(emptyDb.session);

        assertEquals(emptyDb.session.execute("select * from db_version").one().getInt("version"), 1);

        UUID tracking_id = UUID.randomUUID();

        emptyDb.session.execute("insert into request_status(tracking_id, callback_routing_key, created_date, api_id, request_type, response, status) " +
                                "values (" + tracking_id + ", 'test_callback_routing_key', dateof(now()), 12, 'test_request_type', 'test_response', 'test_status')");

        Row row = emptyDb.session.execute("select * from request_status").one();
        assertEquals(row.getUUID("tracking_id"), tracking_id);
        assertEquals(row.getString("callback_routing_key"), "test_callback_routing_key");
        assertEquals(row.getLong("api_id"), 12);
        assertEquals(row.getString("request_type"), "test_request_type");
        assertEquals(row.getString("response"), "test_response");
        assertEquals(row.getString("status"), "test_status");

    }

    @Test(expected=InvalidQueryException.class)
    public void test_drop_tables() throws Exception {

        DbRunnerConfig dbRunnerConfig = DbRunnerConfig.builder()
                                                      .dbVersion(1)
                                                      .filePath("src/test/resources")
                                                      .recreateDatabase(true)
                                                      .build();

        emptyDb.session.execute("CREATE TABLE table_to_drop ( " +
                                "    drop_id int PRIMARY KEY, " +
                                "    drop_date timestamp " +
                                ")");

        assertEquals(emptyDb.session.execute("SELECT * FROM TABLE_TO_DROP").all(), Lists.newArrayList());

        DbScriptsRunner dbScriptsRunner = new DbScriptsRunner(dbRunnerConfig);

        dbScriptsRunner.run(emptyDb.session);

        assertEquals(emptyDb.session.execute("select * from db_version").one().getInt("version"), 1);

        //Throws InvalidQueryException as table no longer exists.
        emptyDb.session.execute("SELECT * FROM TABLE_TO_DROP");
    }

    @After
    public void finished() {
        emptyDb.session.execute("DROP KEYSPACE " + emptyDb.session.getLoggedKeyspace());
    }
}
