package io.paradoxical.cassandra.loader;

import org.apache.commons.lang.StringUtils;
import org.cassandraunit.dataset.ParseException;
import org.cassandraunit.dataset.cql.AbstractCQLDataSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ClassPathListCQLDataSet extends AbstractCQLDataSet {

    private final List<String> dataSetLocations;

    public ClassPathListCQLDataSet(List<String> dataSetLocations) {
        super(null);
        this.dataSetLocations = dataSetLocations;
    }

    @Override protected InputStream getInputDataSetLocation(final String dataSetLocation) {
        return this.getClass().getResourceAsStream("/" + dataSetLocation);
    }

    @Override
    public List<String> getLines() {
        List<String> lines = new ArrayList<>();

        for(String dataSetLocation : dataSetLocations) {
            InputStream inputStream = getInputDataSetLocation(dataSetLocation);
            final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader br = new BufferedReader(inputStreamReader);
            String line;
            try {
                while ((line = br.readLine()) != null) {
                    if (StringUtils.isNotBlank(line)) {
                        lines.add(line);
                    }
                }
                br.close();
            }
            catch (IOException e) {
                throw new ParseException(e);
            }
        }

        return lines;
    }
}
