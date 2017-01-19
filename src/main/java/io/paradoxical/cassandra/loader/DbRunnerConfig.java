package io.paradoxical.cassandra.loader;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DbRunnerConfig {

    private String password;

    private String username;

    private String keyspace;

    private Integer port;

    private String ip;

    private Integer dbVersion;

    private String filePath;

    private Boolean recreateDatabase = false;

    private Boolean createKeyspace = false;

    private String replicationMap;

    public String getFilePath() {
        if(!filePath.endsWith("/")) {
            filePath = filePath + "/";
        }
        return filePath;
    }

}
