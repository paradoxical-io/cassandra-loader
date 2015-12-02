package io.paradoxical.cassandra.loader;


import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.cassandraunit.dataset.CQLDataSet;

import java.util.List;

@Data
@AllArgsConstructor
public class EmptyCqlDataSet implements CQLDataSet {

    private String keyspaceName;

    private boolean keyspaceCreation;

    private boolean keyspaceDeletion;

    @Override public List<String> getCQLStatements() {
        return Lists.newArrayList();
    }

}
