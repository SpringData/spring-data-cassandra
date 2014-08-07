Spring Data Cassandra
=====================

This is a SpringFramework Data project for Cassandra that uses the binary CQL3 protocol via
the official DataStax 2.x Java [driver](https://github.com/datastax/java-driver) for Cassandra 2.0.

Supports native CQL3 queries in SpringFramework Data Repositories.

Versions
--------

Current versions:

 - CQL 3.1

 - Cassandra 2.0

 - Datastax Java Driver 2.0.4


Requirements
--------

  - Java >= 1.6 (OpenJDK and Sun have been tested)

Recommended to use JDK 1.7

CQL
--------

Fully supported CQL 3.0 version. Specification is available [here] (http://www.datastax.com/documentation/cql/3.0/pdf/cql30.pdf)

Modules
--------

This project has two modules:
 - [Spring Data Cql](cql)
 - [Spring Data Cassandra](cassandra)

Spring Data Cql gives ability to use CqlTemplate and makes operations based in CQL.
Spring Data Cassandra gives ability to use CassandraTemplate and CassandraRepository.

Building
--------
This is a standard Maven multimodule project.  Just issue the command `mvn clean install` from the repo root.

Snapshot builds are available [here](https://oss.sonatype.org/index.html#nexus-search;quick~org.springdata)

Using in maven application
-------

Add snapshot repository to pom.xml

```

<repositories>
  <repository>
    <id>sonatype-nexus-snapshots</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
  </repository>
</repositories>

```

Add dependencies to pom.xml

```

<dependency>
  <groupId>org.springdata</groupId>
  <artifactId>spring-data-cql</artifactId>
  <version>2.0.0.BUILD-SNAPSHOT</version>
</dependency>

<dependency>
  <groupId>org.springdata</groupId>
  <artifactId>spring-data-cassandra</artifactId>
  <version>2.0.0.BUILD-SNAPSHOT</version>
</dependency>

```


Continuous integration
--------
https://travis-ci.org/SpringData/spring-data-cassandra


Contribution
--------

A few steps to make this process easy for us:

 - Please use [eclipse-formatting.xml] (eclipse-formatting.xml) or similar formatting for Idea with auto-save option.
 - Create a new issue in https://github.com/SpringData/spring-data-cassandra/issues/ and refer this issue in the push request.
 - Thank you.
