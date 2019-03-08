package org.springframework.data.cassandra.examples.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.AbstractSessionConfiguration;
import org.springframework.data.cassandra.examples.core.io.IOUtils;
import org.springframework.data.cassandra.examples.core.model.Person;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@SpringBootApplication
@EnableCassandraRepositories
@SuppressWarnings("unused")
public class CassandraRepositoryExample implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRepositoryExample.class);

	// Cassandra Database Configuration Properties
	static final int DEFAULT_PORT = 9042;

	static final String HOSTNAME = "localhost";
	static final String KEYSPACE_NAME = "SpringDataCassandraExamples";

	public static void main(String[] args) {
		SpringApplication.run(CassandraRepositoryExample.class, args);
	}

	@Autowired
	private Cluster cluster;

	@Autowired
	private PersonRepository personRepository;

	public void run(String[] args) throws Exception {

		try {

			Person insertedJonDoe = this.personRepository.save(Person.create("Jon Doe", 42));

			LOGGER.info("Inserted [{}]", insertedJonDoe);

			Person queriedJonDoe = this.personRepository.findById(insertedJonDoe.getId()).orElse(null);

			LOGGER.info("Query Result [{}]", queriedJonDoe);

			assertThat(queriedJonDoe).isNotSameAs(insertedJonDoe);
			assertThat(queriedJonDoe).isEqualTo(insertedJonDoe);
		}
		finally {
			IOUtils.close(cluster);
		}
	}
}

interface PersonRepository extends CassandraRepository<Person, String> { }

@Configuration
@SuppressWarnings("unused")
class CassandraConfiguration extends AbstractSessionConfiguration {

	@Autowired
	Environment env;

	@Override
	protected String getContactPoints() {
		return env.getProperty("cassandra.cluster.contact-points", CassandraRepositoryExample.HOSTNAME);
	}

	@Override
	protected String getKeyspaceName() {
		return env.getProperty("cassandra.keyspace", CassandraRepositoryExample.KEYSPACE_NAME);
	}

	@Override
	protected int getPort() {
		return env.getProperty("cassandra.cluster.port", Integer.TYPE, CassandraRepositoryExample.DEFAULT_PORT);
	}

}
