/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springdata.cassandra.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springdata.cassandra.convert.CassandraConverter;
import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.mapping.CassandraPersistentProperty;
import org.springdata.cql.core.CqlOperations;
import org.springdata.cql.core.CqlTemplate;
import org.springdata.cql.core.ResultSetExtractor;
import org.springdata.cql.core.RowMapper;
import org.springdata.cql.core.RowMapperResultSetExtractor;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * The CassandraTemplate is a convenience API for all CassandraOperations using POJOs. This is the "Spring Data" flavor
 * of the template. For low level CassandraOperations use the {@link CqlTemplate}
 * 
 * @author Alex Shvid
 * @author David Webb
 */
public class CassandraTemplate implements CassandraOperations {

	// private final static Logger logger = LoggerFactory.getLogger(CassandraTemplate.class);

	/*
	 * List of iterable classes when testing POJOs for specific operations.
	 */
	public static final Collection<String> ITERABLE_CLASSES;
	static {

		Set<String> iterableClasses = new HashSet<String>();
		iterableClasses.add(List.class.getName());
		iterableClasses.add(Collection.class.getName());
		iterableClasses.add(Iterator.class.getName());

		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);

	}

	private final CqlTemplate cqlTemplate;
	private final CassandraConverter cassandraConverter;
	private final String keyspace;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final SchemaOperations schemaDataOperations;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraTemplate(Session session, CassandraConverter converter, String keyspace) {
		Assert.notNull(session);
		Assert.notNull(converter);
		this.cqlTemplate = new CqlTemplate(session, keyspace);
		this.cassandraConverter = converter;
		this.keyspace = keyspace;
		this.mappingContext = this.cassandraConverter.getMappingContext();
		this.schemaDataOperations = new DefaultSchemaOperations(this);
	}

	@Override
	public <T> List<T> findAll(Class<T> entityClass) {
		return buildFindAllOperation(entityClass).execute();
	}

	@Override
	public <T> GetOperation<List<T>> buildFindAllOperation(Class<T> entityClass) {
		Assert.notNull(entityClass);

		return new AbstractFindOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				Select select = QueryBuilder.select().all().from(cassandraTemplate.getKeyspace(), getTableName());
				return select;
			}

		};
	}

	@Override
	public <T> List<T> findAll(Class<T> entityClass, Iterable<?> ids) {
		return buildFindAllOperation(entityClass, ids).execute();
	}

	@Override
	public <T> GetOperation<List<T>> buildFindAllOperation(final Class<T> entityClass, final Iterable<?> ids) {
		Assert.notNull(entityClass);
		Assert.notNull(ids);

		return new DefaultMultiFindOperation<T>(this, entityClass, ids.iterator());
	}

	@Override
	public <T> T findById(Class<T> entityClass, Object id) {
		return buildFindByIdOperation(entityClass, id).execute();
	}

	@Override
	public <T> GetOperation<T> buildFindByIdOperation(Class<T> entityClass, final Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);

		return new AbstractFindOneOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				Select select = QueryBuilder.select().all().from(cassandraTemplate.getKeyspace(), getTableName());
				Select.Where w = select.where();

				CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

				List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

				for (Clause c : list) {
					w.and(c);
				}

				return select;
			}

		};

	}

	@Override
	public <T> List<T> findByPartitionKey(Class<T> entityClass, Object id) {
		return buildFindByPartitionKeyOperation(entityClass, id).execute();
	}

	@Override
	public <T> GetOperation<List<T>> buildFindByPartitionKeyOperation(Class<T> entityClass, final Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);

		return new AbstractFindOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				Select select = QueryBuilder.select().all().from(cassandraTemplate.getKeyspace(), getTableName());
				Select.Where w = select.where();

				CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

				List<Clause> list = cassandraConverter.getPartitionKey(entity, id);

				for (Clause c : list) {
					w.and(c);
				}

				return select;
			}

		};

	}

	@Override
	public <T> List<T> find(Class<T> entityClass, String cql) {
		return buildFindOperation(entityClass, cql).execute();
	}

	@Override
	public <T> GetOperation<List<T>> buildFindOperation(Class<T> entityClass, final String cql) {
		Assert.notNull(entityClass);
		Assert.notNull(cql);

		return new AbstractFindOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				return new SimpleStatement(cql);
			}

		};

	}

	@Override
	public <T> T findOne(Class<T> entityClass, String cql) {
		return buildFindOneOperation(entityClass, cql).execute();
	}

	@Override
	public <T> GetOperation<T> buildFindOneOperation(Class<T> entityClass, final String cql) {
		Assert.notNull(entityClass);
		Assert.notNull(cql);

		return new AbstractFindOneOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				return new SimpleStatement(cql);
			}

		};

	}

	@Override
	public <T> ResultSet deleteByIdInBatch(Class<T> entityClass, Iterable<?> ids) {
		return buildDeleteByIdInBatchOperation(entityClass, ids).execute();
	}

	@Override
	public <T> BatchOperation buildDeleteByIdInBatchOperation(final Class<T> entityClass, Iterable<?> ids) {
		Assert.notNull(entityClass);
		Assert.notNull(ids);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(ids.iterator(),
				new Function<Object, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(Object id) {

						Assert.notNull(id);
						assertNotIterable(id);

						return new DefaultDeleteOperation<T>(cassandraTemplate, entityClass, id);
					}

				});

		return new DefaultBatchOperation(this, creators);
	}

	@Override
	public <T> ResultSet deleteById(Class<T> entityClass, Object id) {
		return buildDeleteByIdOperation(entityClass, id).execute();
	}

	@Override
	public <T> DefaultDeleteOperation<T> buildDeleteByIdOperation(Class<T> entityClass, Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);
		assertNotIterable(id);
		return new DefaultDeleteOperation<T>(this, entityClass, id);
	}

	@Override
	public <T> ResultSet deleteInBatch(Iterable<T> entities) {
		return buildDeleteInBatchOperation(entities).execute();
	}

	@Override
	public <T> BatchOperation buildDeleteInBatchOperation(Iterable<T> entities) {
		Assert.notNull(entities);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(entities.iterator(),
				new Function<T, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(T entity) {

						Assert.notNull(entity);
						assertNotIterable(entity);

						return new DefaultDeleteOperation<T>(cassandraTemplate, entity);
					}

				});

		return new DefaultBatchOperation(this, creators);
	}

	@Override
	public <T> ResultSet delete(T entity) {
		return buildDeleteOperation(entity).execute();
	}

	@Override
	public <T> DeleteOperation buildDeleteOperation(T entity) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		return new DefaultDeleteOperation<T>(this, entity);
	}

	@Override
	public <T> ResultSet deleteAll(Class<T> entityClass) {
		return buildDeleteAllOperation(entityClass).execute();
	}

	@Override
	public <T> DeleteOperation buildDeleteAllOperation(Class<T> entityClass) {
		Assert.notNull(entityClass);
		return new DefaultDeleteOperation<T>(this, entityClass);
	}

	@Override
	public <T> Long countAll(Class<T> entityClass) {
		return buildCountAllOperation(entityClass).execute();
	}

	@Override
	public <T> GetOperation<Long> buildCountAllOperation(Class<T> entityClass) {
		Assert.notNull(entityClass);
		return new DefaultCountOperation<T>(this, entityClass);
	}

	@Override
	public <T> Boolean exists(T entity) {
		return buildExistsOperation(entity).execute();
	}

	@Override
	public <T> GetOperation<Boolean> buildExistsOperation(T entity) {
		Assert.notNull(entity);
		return new DefaultExistsOperation<T>(this, entity);
	}

	@Override
	public <T> Boolean exists(Class<T> entityClass, Object id) {
		return buildExistsOperation(entityClass, id).execute();
	}

	@Override
	public <T> GetOperation<Boolean> buildExistsOperation(Class<T> entityClass, Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);
		return new DefaultExistsOperation<T>(this, entityClass, id);
	}

	/**
	 * @param entityClass
	 * @return
	 */
	public String determineTableName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
		}
		return entity.getTableName();
	}

	@Override
	public CassandraConverter getConverter() {
		return cassandraConverter;
	}

	@Override
	public String getKeyspace() {
		return keyspace;
	}

	@Override
	public String getTableName(Class<?> entityClass) {
		return determineTableName(entityClass);
	}

	@Override
	public <T> ResultSet saveNewInBatch(Iterable<T> entities) {
		return buildSaveNewInBatchOperation(entities).execute();
	}

	@Override
	public <T> BatchOperation buildSaveNewInBatchOperation(Iterable<T> entities) {
		Assert.notNull(entities);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(entities.iterator(),
				new Function<T, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(T entity) {

						Assert.notNull(entity);
						assertNotIterable(entity);

						return new DefaultSaveNewOperation<T>(cassandraTemplate, entity);
					}

				});

		return new DefaultBatchOperation(this, creators);

	}

	@Override
	public <T> ResultSet saveNew(T entity) {
		return buildSaveNewOperation(entity).execute();
	}

	@Override
	public <T> SaveNewOperation buildSaveNewOperation(T entity) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		return new DefaultSaveNewOperation<T>(this, entity);
	}

	@Override
	public <T> ResultSet saveInBatch(Iterable<T> entities) {
		return buildSaveInBatchOperation(entities).execute();
	}

	@Override
	public <T> BatchOperation buildSaveInBatchOperation(Iterable<T> entities) {
		Assert.notNull(entities);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(entities.iterator(),
				new Function<T, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(T entity) {

						Assert.notNull(entity);
						assertNotIterable(entity);

						return new DefaultSaveOperation<T>(cassandraTemplate, entity);
					}

				});

		return new DefaultBatchOperation(this, creators);
	}

	@Override
	public <T> ResultSet save(T entity) {
		return buildSaveOperation(entity).execute();
	}

	@Override
	public <T> SaveOperation buildSaveOperation(T entity) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		return new DefaultSaveOperation<T>(this, entity);
	}

	@Override
	public <T> RowMapper<T> getRowMapperFor(Class<T> entityClass) {
		Assert.notNull(entityClass);
		return new ReaderRowMapper<T>(cassandraConverter, entityClass);
	}

	@Override
	public <T> ResultSetExtractor<List<T>> getResultSetExtractorFor(Class<T> entityClass) {
		return new RowMapperResultSetExtractor<T>(getRowMapperFor(entityClass));
	}

	@Override
	public <T> List<T> process(ResultSet resultSet, Class<T> entityClass) {
		Assert.notNull(resultSet);
		Assert.notNull(entityClass);

		return cqlTemplate().process(resultSet, new ReaderRowMapper<T>(cassandraConverter, entityClass));
	}

	@Override
	public <T> void process(ResultSet resultSet, Class<T> entityClass, final EntryCallbackHandler<T> ech) {
		Assert.notNull(resultSet);
		Assert.notNull(entityClass);
		Assert.notNull(ech);

		cqlTemplate().process(resultSet, new ReaderEntryCallbackAdapter<T>(cassandraConverter, entityClass, ech));
	}

	@Override
	public <T> T processOne(ResultSet resultSet, Class<T> entityClass, boolean singleResult) {
		Assert.notNull(resultSet);
		Assert.notNull(entityClass);

		return cqlTemplate().processOne(resultSet, new ReaderRowMapper<T>(cassandraConverter, entityClass), singleResult);
	}

	@Override
	public SchemaOperations getSchemaOperations() {
		return schemaDataOperations;
	}

	@Override
	public CqlOperations getCqlOperations() {
		return cqlTemplate;
	}

	public CqlTemplate cqlTemplate() {
		return cqlTemplate;
	}

	/**
	 * @param obj
	 * @return
	 */
	protected <T> String determineTableName(T obj) {
		if (null != obj) {
			return determineTableName(obj.getClass());
		}

		return null;
	}

	/**
	 * Verify the object is not an iterable type
	 * 
	 * @param o
	 */
	protected void assertNotIterable(Object o) {
		if (null != o) {
			if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}

	/**
	 * Service method for persistent entity lookup
	 * 
	 * @param entityClass
	 * @return CassandraPertistentEntity
	 */

	protected CassandraPersistentEntity<?> getPersistentEntity(Class<?> entityClass) {

		if (entityClass == null) {
			throw new IllegalArgumentException("No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		if (entity == null) {
			throw new MappingException("persistent entity not found for a given class " + entityClass);
		}

		return entity;
	}

	/**
	 * Service method used to translate exceptions
	 * 
	 * @param ex RuntimeException
	 */
	protected RuntimeException translateIfPossible(RuntimeException ex) {
		return cqlTemplate.translateIfPossible(ex);
	}
}
