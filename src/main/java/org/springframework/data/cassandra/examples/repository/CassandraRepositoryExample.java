package org.springframework.data.cassandra.examples.repository;

import static org.assertj.core.api.Assertions.assertThat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.examples.core.io.IOUtils;
import org.springframework.data.cassandra.examples.core.model.Person;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.TypedIdCassandraRepository;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import com.datastax.driver.core.Cluster;

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
			Person insertedJonDoe = personRepository.save(Person.create("Jon Doe", 42));

			LOGGER.info("Inserted [{}]", insertedJonDoe);

			Person queriedJonDoe = personRepository.findOne(insertedJonDoe.getId());

			LOGGER.info("Query Result [{}]", queriedJonDoe);

			assertThat(queriedJonDoe).isNotSameAs(insertedJonDoe);
			assertThat(queriedJonDoe).isEqualTo(insertedJonDoe);
		}
		finally {
			IOUtils.close(cluster);
		}
	}
}

interface PersonRepository extends TypedIdCassandraRepository<Person, String> {
}

@Configuration
@SuppressWarnings("unused")
class CassandraConfiguration {

	@Autowired
	Environment env;

	@Bean
	CassandraClusterFactoryBean cluster() {
		CassandraClusterFactoryBean cassandraCluster = new CassandraClusterFactoryBean();

		cassandraCluster.setContactPoints(env.getProperty("cassandra.cluster.contact-points",
			CassandraRepositoryExample.HOSTNAME));
		cassandraCluster.setPort(env.getProperty("cassandra.cluster.port", Integer.TYPE,
			CassandraRepositoryExample.DEFAULT_PORT));

		return cassandraCluster;
	}

	@Bean
	CassandraMappingContext mappingContext() {
		return new BasicCassandraMappingContext();
	}

	@Bean
	CassandraConverter converter() {
		return new MappingCassandraConverter(mappingContext());
	}

	@Bean
	CassandraSessionFactoryBean session() throws Exception {
		CassandraSessionFactoryBean cassandraSession = new CassandraSessionFactoryBean();

		cassandraSession.setCluster(cluster().getObject());
		cassandraSession.setConverter(converter());
		cassandraSession.setKeyspaceName(env.getProperty("cassandra.keyspace",
			CassandraRepositoryExample.KEYSPACE_NAME));
		cassandraSession.setSchemaAction(SchemaAction.NONE);

		return cassandraSession;
	}
}
