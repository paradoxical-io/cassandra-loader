cassandra-loader
========================

![Build status](https://travis-ci.org/paradoxical-io/cassandra-loader.svg?branch=master)


This is a simple cassandra migration runner. It will manage your cassandra keyspace versions by keeping track of a 
dbversion and letting you run versioned db scripts. Leverage this project to also run in memory cassandra migrations 
(if you have tests you want to run for your cassandra scripts).

To run the standalone runner see below.

You will need java 8 and maven installed.  For mac users brew install the stuff.

## How to Run:

```
> ./cassandra-loader.sh

Unexpected exception:Missing required options: ip, u, pw, k
usage: Main
 -f,--file-path <arg>         CQL File Path (default =
                              ../db/src/main/resources)
 -ip <arg>                    Cassandra IP Address
 -k,--keyspace <arg>          Cassandra Keyspace
 -p,--port <arg>              Cassandra Port (default = 9042)
 -pw,--password <arg>         Cassandra Password
 -recreateDatabase            Deletes all tables. WARNING all
                              data will be deleted! 
 -u,--username <arg>          Cassandra Username
 -v,--upgrade-version <arg>   Upgrade to Version
```