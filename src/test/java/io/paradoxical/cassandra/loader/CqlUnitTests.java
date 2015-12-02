package io.paradoxical.cassandra.loader;

import io.paradoxical.cassandra.loader.db.CqlUnitDb;
import org.junit.Test;

public class CqlUnitTests {
    @Test
    public void test_cqlunit_two_clients() throws Exception {
        CqlUnitDb.create("");
        CqlUnitDb.create("");
    }
}
