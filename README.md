cassandra-loader
========================

[![Build Status](https://travis-ci.org/paradoxical-io/cassandra-loader.svg?branch=master)](https://travis-ci.org/paradoxical-io/cassandra-loader)
[![Maven Central](https://img.shields.io/maven-central/v/io.paradoxical/cassandra.loader.svg?maxAge=2592000)](https://github.com/paradoxical-io/cassandra-loader/releases)

This is a simple cassandra migration runner. It will manage your cassandra keyspace versions by keeping track of a 
dbversion and letting you run versioned db scripts. Leverage this project to also run in memory cassandra migrations 
(if you have tests you want to run for your cassandra scripts).

# Cql Scripts

Cassandra loader works by tracking your db version and applying lexically ordered cql scripts against the current db.

For example, if you have a folder like:

```
01_initial.cql
02_add_users.cql
03_add_tracking_column.cql
```

This corresponds to DB version 3.  Cassandra loader will apply migrations only when needed, so if you already are at db version 3 nothing happens. If your db version table is at 2, then the 03 script is run.

When you first set up a keyspace with the loader, the loader will bootstrap a db_version table to track this for you. It tracks the history of when scripts were applied and which version its at now.

# Installation

## In process upgrades

```
<dependency>
    <groupId>io.paradoxical</groupId>
    <artifactId>cassandra.loader</artifactId>
    <version>1.3</version>
</dependency>
```

The loader allows you to create a cassandra session instance from a db folder of cql scripts. For example:

```
public static Session create() throws Exception {
    return CqlUnitDb.create("../db/scripts");
}
```

This will create a fresh DB based on your cql scripts.  The db created with this function is *cached* because creating sessions isn't a lightweight process.  To create a fresh db do the following

```
public static Session createFresh() throws Exception {
    return CqlUnitDb.unCached("../db/scripts");
}
```

## Standalone running:

You can also run the loader as a standalone jar.  This way you can use it as part of a CICD pipeline, instead of in integration tests.

To install:

```
<dependency>
    <groupId>io.paradoxical</groupId>
    <artifactId>cassandra.loader</artifactId>
    <version>1.1</version>
    <classifier>runner</classifier>
</dependency>
```

Or pull it [directly from maven](https://repo1.maven.org/maven2/io/paradoxical/cassandra.loader/1.1/cassandra.loader-1.1-runner.jar).

To use it:


```
> java -jar cassandra.loader-runner.jar

Unexpected exception:Missing required options: ip, u, pw, k
Usage: Main
 -createKeyspace              Creates the keyspace
 --debug                      Optional debug flag. Spits out all logs
 -f,--file-path <arg>         CQL File Path (default =
                              ../db/src/main/resources)
 -ip <arg>                    Cassandra IP Address
 -k,--keyspace <arg>          Cassandra Keyspace
 -p,--port <arg>              Cassandra Port (default = 9042)
 -pw,--password <arg>         Cassandra Password
 -recreateDatabase            Deletes all tables. WARNING all data will be
                              deleted!
 -u,--username <arg>          Cassandra Username
 -v,--upgrade-version <arg>   Upgrade to Version
