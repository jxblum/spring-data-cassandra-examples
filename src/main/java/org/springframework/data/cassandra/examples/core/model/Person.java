package org.springframework.data.cassandra.examples.core.model;

import java.util.UUID;

import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

@Table("People")
public class Person {

	private static final String PERSON_TO_STRING = "{ @type = %1$s, id = %2$s, name = %3$s, age = %4$d }";

	@PrimaryKey
	private final String id;

	private final String name;

	private int age;

	public static Person create(String name, int age) {
		return create(UUID.randomUUID().toString(), name, age);
	}

	public static Person create(String id, String name, int age) {
		return new Person(id, name, age);
	}

	@PersistenceConstructor
	public Person(String id, String name, int age) {

		Assert.hasText(id, "id must be set");
		Assert.hasText(name, "name must be set");

		this.id = id;
		this.name = name;
		this.age = validateAge(age);
	}

	private int validateAge(int age) {
		Assert.isTrue(age > 0, "age must be greater than 0");
		return age;
	}

	public String getId() {
		return this.id;
	}

	public String getName() {
		return this.name;
	}

	public int getAge() {
		return this.age;
	}

	public void setAge(int age) {
		this.age = validateAge(age);
	}

	@Override
	public boolean equals(Object obj) {

		if (obj == this) {
			return true;
		}

		if (!(obj instanceof Person)) {
			return false;
		}

		Person that = (Person) obj;

		return ObjectUtils.nullSafeEquals(this.getId(), that.getId())
			|| (ObjectUtils.nullSafeEquals(this.getName(), that.getName())
			&& ObjectUtils.nullSafeEquals(this.getAge(), that.getAge()));
	}

	@Override
	public int hashCode() {

		int hashValue = 17;

		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getId());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getName());
		hashValue = 37 * hashValue + ObjectUtils.nullSafeHashCode(this.getAge());

		return hashValue;
	}

	@Override
	public String toString() {
		return String.format(PERSON_TO_STRING, getClass().getName(), getId(), getName(), getAge());
	}
}
