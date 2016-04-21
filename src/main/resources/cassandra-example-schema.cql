CREATE KEYSPACE SpringDataCassandraExamples WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
CREATE TABLE SpringDataCassandraExamples.People (id VARCHAR PRIMARY KEY, name VARCHAR, age INT);
USE SpringDataCassandraExamples;
