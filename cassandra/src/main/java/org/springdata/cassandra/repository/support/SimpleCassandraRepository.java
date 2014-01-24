/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springdata.cassandra.repository.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springdata.cassandra.core.CassandraOperations;
import org.springdata.cassandra.core.CassandraTemplate;
import org.springdata.cassandra.repository.CassandraRepository;
import org.springdata.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableList;

/**
 * Simple Repository implementation for Cassandra.
 * 
 * @author Alex Shvid
 * 
 */

public class SimpleCassandraRepository<T, ID extends Serializable> implements CassandraRepository<T, ID> {

	private final CassandraTemplate cassandraTemplate;
	private final CassandraEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link CassandraTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public SimpleCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraTemplate cassandraTemplate) {

		Assert.notNull(cassandraTemplate);
		Assert.notNull(metadata);

		this.entityInformation = metadata;
		this.cassandraTemplate = cassandraTemplate;
	}

	@Override
	public <S extends T> S save(S entity) {
		Assert.notNull(entity, "Entity must not be null!");
		cassandraTemplate.saveNew(entity).execute();
		return entity;
	}

	@Override
	public <S extends T> List<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		cassandraTemplate.saveNewInBatch(entities).execute();

		if (entities instanceof List) {
			return (List<S>) entities;
		} else {
			return ImmutableList.copyOf(entities);
		}
	}

	@Override
	public T findOne(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return cassandraTemplate.findById(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public List<T> findByPartitionKey(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		Iterator<T> iterator = cassandraTemplate.findByPartitionKey(entityInformation.getJavaType(), id).execute();
		return ImmutableList.copyOf(iterator);
	}

	@Override
	public boolean exists(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return cassandraTemplate.exists(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public long count() {
		Long result = cassandraTemplate.cqlOps().countAll(entityInformation.getTableName()).execute();
		return result != null ? result : 0;
	}

	@Override
	public void delete(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		cassandraTemplate.deleteById(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public void delete(T entity) {
		Assert.notNull(entity, "The given entity must not be null!");
		cassandraTemplate.delete(entity).execute();
	}

	@Override
	public void delete(Iterable<? extends T> entities) {
		Assert.notNull(entities, "The given Iterable of entities not be null!");
		cassandraTemplate.deleteInBatch(entities).execute();
	}

	@Override
	public void deleteAll() {
		cassandraTemplate.cqlOps().truncate(entityInformation.getTableName()).execute();
	}

	@Override
	public List<T> findAll() {
		Iterator<T> iterator = cassandraTemplate.findAll(entityInformation.getJavaType()).execute();
		return ImmutableList.copyOf(iterator);
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {

		List<ID> parameters = new ArrayList<ID>();
		for (ID id : ids) {
			parameters.add(id);
		}
		Clause clause = QueryBuilder.in(entityInformation.getIdColumn(), parameters.toArray());
		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		select.where(clause);

		return findAll(select);
	}

	private List<T> findAll(Select query) {

		if (query == null) {
			return Collections.emptyList();
		}

		return cassandraTemplate.find(query.getQueryString(), entityInformation.getJavaType(), null);
	}

	/**
	 * Returns the underlying {@link CassandraOperations} instance.
	 * 
	 * @return
	 */
	protected CassandraOperations getCassandraOperations() {
		return this.cassandraTemplate;
	}

	/**
	 * @return the entityInformation
	 */
	protected CassandraEntityInformation<T, ID> getEntityInformation() {
		return entityInformation;
	}

}
