package org.springframework.data.cassandra.examples.template;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.UnknownHostException;
import java.util.Collections;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.cassandra.CassandraTypeMismatchException;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.examples.core.io.IOUtils;
import org.springframework.data.cassandra.examples.core.model.Person;
import org.springframework.data.cassandra.examples.core.net.NetworkUtils;
import org.springframework.util.StringUtils;

public class CassandraTemplateExample {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTemplateExample.class);

	// Cassandra Database Configuration Properties
	static final int DEFAULT_PORT = 9042;

	static final String HOSTNAME = "localhost";
	static final String KEYSPACE_NAME = "SpringDataCassandraExamples";

	// 'People' Table Column Names
	static final String AGE_COLUMN_NAME = "age";
	static final String ID_COLUMN_NAME = "id";
	static final String NAME_COLUMN_NAME = "name";

	static Cluster cluster;

	public static void main(String[] args) throws UnknownHostException {

		try {

			CassandraOperations template = new CassandraTemplate(connect(HOSTNAME, KEYSPACE_NAME));

			Person insertedJonDoe = template.insert(Person.create("Jane Doe", 37));

			LOGGER.info("Inserted [{}]", insertedJonDoe);

			Select personQuery = selectPerson(insertedJonDoe.getId());

			LOGGER.info("CQL SELECT [{}]", personQuery);

			Person queriedJonDoe = template.getCqlOperations().queryForObject(personQuery, personRowMapper());

			LOGGER.info("Query Result [{}]", queriedJonDoe);

			assertThat(queriedJonDoe).isNotSameAs(insertedJonDoe);
			assertThat(queriedJonDoe).isEqualTo(insertedJonDoe);
		}
		finally {
			IOUtils.close(cluster);
		}
	}

	protected static Session connect(String hostname, String keyspace) {
		return connect(hostname, DEFAULT_PORT, keyspace);
	}

	protected static synchronized Session connect(String hostname, int port, String keyspace) {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPointsWithPorts(Collections.singleton(
				NetworkUtils.newSocketAddress(hostname, port))).build();
		}

		return cluster.connect(keyspace);
	}

	protected static RowMapper<Person> personRowMapper() {

		return new RowMapper<Person>() {

			public Person mapRow(Row row, int rowNum) throws DriverException {

				try {

					LOGGER.debug("row [{}] @ index [{}]", row, rowNum);

					Person person = Person.create(row.getString(ID_COLUMN_NAME),
						row.getString(NAME_COLUMN_NAME), row.getInt(AGE_COLUMN_NAME));

					LOGGER.debug("person [{}]", person);

					return person;
				}
				catch (Exception cause) {
					throw new CassandraTypeMismatchException(String.format(
						"failed to map row [%1$] @ index [%2$d] to object of type [%3$s]",
						row, rowNum, Person.class.getName()), cause);
				}
			}
		};
	}

	protected static Select selectPerson(String personId) {

		Select selectStatement = QueryBuilder.select().from(toTableName(Person.class));

		selectStatement.where(QueryBuilder.eq(ID_COLUMN_NAME, personId));

		return selectStatement;
	}

	@SuppressWarnings("unused")
	protected static String toTableName(Object obj) {
		return toTableName(obj.getClass());
	}

	protected static String toTableName(Class<?> type) {

		Table tableAnnotation = type.getAnnotation(Table.class);

		return tableAnnotation != null && StringUtils.hasText(tableAnnotation.value())
			? tableAnnotation.value()
			: type.getSimpleName();
	}
}
