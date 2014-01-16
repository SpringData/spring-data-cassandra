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
import java.util.List;

import org.springdata.cassandra.core.CassandraOperations;
import org.springdata.cassandra.core.CassandraTemplate;
import org.springdata.cassandra.repository.CassandraRepository;
import org.springdata.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Repository base implementation for Cassandra.
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

		List<S> result = new ArrayList<S>();

		for (S entity : entities) {
			save(entity);
			result.add(entity);
		}

		return result;
	}

	private Clause getIdClause(ID id) {
		Clause clause = QueryBuilder.eq(entityInformation.getIdColumn(), id);
		return clause;
	}

	@Override
	public T findOne(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return cassandraTemplate.findById(id, entityInformation.getJavaType(), null);
	}

	@Override
	public List<T> findByPartitionKey(ID id) {
		Assert.notNull(id, "The given id must not be null!");

		return cassandraTemplate.findByPartitionKey(id, entityInformation.getJavaType(), null);
	}

	@Override
	public boolean exists(ID id) {

		Assert.notNull(id, "The given id must not be null!");

		Select select = QueryBuilder.select().countAll().from(entityInformation.getTableName());
		select.where(getIdClause(id));

		Long num = cassandraTemplate.count(select.getQueryString(), null);
		return num != null && num.longValue() > 0;
	}

	@Override
	public long count() {
		return cassandraTemplate.countAll(entityInformation.getTableName(), null);
	}

	@Override
	public void delete(ID id) {
		Assert.notNull(id, "The given id must not be null!");

		cassandraTemplate.deleteById(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public void delete(T entity) {
		Assert.notNull(entity, "The given entity must not be null!");
		delete(entityInformation.getId(entity));
	}

	@Override
	public void delete(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		for (T entity : entities) {
			delete(entity);
		}
	}

	@Override
	public void deleteAll() {
		cassandraTemplate.cqlOps().truncate(entityInformation.getTableName()).execute();
	}

	@Override
	public List<T> findAll() {
		Select select = QueryBuilder.select().all().from(entityInformation.getTableName());
		return findAll(select);
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
