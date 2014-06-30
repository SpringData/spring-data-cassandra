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
import java.util.List;

import org.springdata.cassandra.core.CassandraOperations;
import org.springdata.cassandra.core.CassandraTemplate;
import org.springdata.cassandra.repository.CassandraRepository;
import org.springdata.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.util.Assert;

import com.google.common.collect.ImmutableList;

/**
 * Simple Repository implementation for Cassandra.
 * 
 * @author Alex Shvid
 * 
 */

public class SimpleCassandraRepository<T, ID extends Serializable> implements CassandraRepository<T, ID> {

	private final CassandraEntityInformation<T, ID> entityInformation;
	private final Class<?> repositoryInterface;
	private final CassandraTemplate cassandraTemplate;

	/**
	 * Creates a new {@link SimpleCassandraRepository} for the given {@link CassandraEntityInformation} and
	 * {@link CassandraTemplate}.
	 * 
	 * @param metadata must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public SimpleCassandraRepository(CassandraEntityInformation<T, ID> entityInformation, Class<?> repositoryInterface,
			CassandraTemplate cassandraTemplate) {

		Assert.notNull(entityInformation);
		Assert.notNull(repositoryInterface);
		Assert.notNull(cassandraTemplate);

		this.entityInformation = entityInformation;
		this.repositoryInterface = repositoryInterface;
		this.cassandraTemplate = cassandraTemplate;

	}

	@Override
	public <S extends T> S save(S entity) {
		Assert.notNull(entity, "Entity must not be null!");
		cassandraTemplate.getSaveNewOperation(entity).execute();
		return entity;
	}

	@Override
	public <S extends T> List<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities must not be null!");
		cassandraTemplate.getSaveNewInBatchOperation(entities).execute();

		if (entities instanceof List) {
			return (List<S>) entities;
		} else {
			return ImmutableList.copyOf(entities);
		}
	}

	@Override
	public T findOne(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return cassandraTemplate.getFindByIdOperation(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public List<T> findByPartitionKey(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return cassandraTemplate.getFindByPartitionKeyOperation(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public boolean exists(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return cassandraTemplate.getExistsOperation(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public long count() {
		Long result = cassandraTemplate.getCountAllOperation(entityInformation.getJavaType()).execute();
		return result != null ? result : 0;
	}

	@Override
	public void delete(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		cassandraTemplate.getDeleteByIdOperation(entityInformation.getJavaType(), id).execute();
	}

	@Override
	public void delete(T entity) {
		Assert.notNull(entity, "The given entity must not be null!");
		cassandraTemplate.getDeleteOperation(entity).execute();
	}

	@Override
	public void delete(Iterable<? extends T> entities) {
		Assert.notNull(entities, "The given Iterable of entities not be null!");
		cassandraTemplate.getDeleteInBatchOperation(entities).execute();
	}

	@Override
	public void deleteAll() {
		cassandraTemplate.getDeleteAllOperation(entityInformation.getJavaType()).execute();
	}

	@Override
	public List<T> findAll() {
		return cassandraTemplate.getFindAllOperation(entityInformation.getJavaType()).execute();
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		Assert.notNull(ids, "The given Iterable of ids not be null!");
		return cassandraTemplate.getFindAllOperation(entityInformation.getJavaType(), ids).execute();
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
