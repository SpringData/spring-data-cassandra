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
package org.springdata.cassandra.data.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springdata.cassandra.base.core.CassandraTemplate;
import org.springdata.cassandra.base.core.SessionCallback;
import org.springdata.cassandra.base.core.query.QueryOptions;
import org.springdata.cassandra.data.convert.CassandraConverter;
import org.springdata.cassandra.data.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.data.mapping.CassandraPersistentProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

/**
 * The Cassandra Data Template is a convenience API for all Cassandra Operations using POJOs. This is the "Spring Data"
 * flavor of the template. For low level Cassandra Operations use the {@link CassandraTemplate}
 * 
 * @author Alex Shvid
 * @author David Webb
 */
public class CassandraDataTemplate extends CassandraTemplate implements CassandraDataOperations {

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

	private String keyspace;
	private CassandraConverter cassandraConverter;
	private MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraDataTemplate(Session session, CassandraConverter converter, String keyspace) {
		super(session);
		Assert.notNull(converter);
		Assert.notNull(keyspace);
		this.keyspace = keyspace;
		this.cassandraConverter = converter;
		this.mappingContext = this.cassandraConverter.getMappingContext();
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass, QueryOptions optionsOrNull) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		return findById(id, entityClass, tableName, optionsOrNull);
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass, String tableName, QueryOptions optionsOrNull) {
		Assert.notNull(id);
		assertNotIterable(id);
		Assert.notNull(entityClass);
		Assert.notNull(tableName);
		return doFindById(id, entityClass, tableName, optionsOrNull);
	}

	@Override
	public Long countByQuery(Select selectQuery) {
		return doSelectCount(selectQuery);
	}

	@Override
	public Long count(String tableName) {
		Select select = QueryBuilder.select().countAll().from(tableName);
		return doSelectCount(select);
	}

	@Override
	public <T> void deleteInBatchById(boolean asychronously, Iterable<T> ids, Class<?> entityClass,
			QueryOptions optionsOrNull) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteInBatchById(asychronously, ids, entityClass, tableName, optionsOrNull);
	}

	@Override
	public <T> void deleteInBatchById(boolean asychronously, Iterable<T> ids, Class<?> entityClass, String tableName,
			QueryOptions optionsOrNull) {
		Assert.notNull(ids);
		Assert.notNull(entityClass);
		Assert.notNull(tableName);
		doBatchDeleteById(asychronously, tableName, ids, entityClass, optionsOrNull);
	}

	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass, QueryOptions optionsOrNull) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteById(asychronously, id, entityClass, tableName, optionsOrNull);
	}

	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass, String tableName,
			QueryOptions optionsOrNull) {
		Assert.notNull(id);
		assertNotIterable(id);
		Assert.notNull(entityClass);
		Assert.notNull(tableName);
		doDeleteById(asychronously, tableName, id, entityClass, optionsOrNull);
	}

	@Override
	public <T> void deleteInBatch(boolean asychronously, Iterable<T> entities, QueryOptions optionsOrNull) {
		Assert.notNull(entities);
		doBatchDelete(asychronously, null, entities, optionsOrNull);
	}

	@Override
	public <T> void deleteInBatch(boolean asychronously, Iterable<T> entities, String tableName,
			QueryOptions optionsOrNull) {
		Assert.notNull(entities);
		Assert.notNull(tableName);
		doBatchDelete(asychronously, tableName, entities, optionsOrNull);
	}

	@Override
	public <T> void delete(boolean asychronously, T entity, QueryOptions optionsOrNull) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(asychronously, entity, tableName, optionsOrNull);
	}

	@Override
	public <T> void delete(boolean asychronously, T entity, String tableName, QueryOptions optionsOrNull) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		Assert.notNull(tableName);
		doDelete(asychronously, tableName, entity, optionsOrNull);
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
		return entity.getTable();
	}

	@Override
	public CassandraConverter getConverter() {
		return cassandraConverter;
	}

	@Override
	public String getTableName(Class<?> entityClass) {
		return determineTableName(entityClass);
	}

	@Override
	public <T> void saveNewInBatch(boolean asychronously, Iterable<T> entities, QueryOptions optionsOrNull) {
		Assert.notNull(entities);
		doBatchInsert(asychronously, null, entities, optionsOrNull);
	}

	@Override
	public <T> void saveNewInBatch(boolean asychronously, Iterable<T> entities, String tableName,
			QueryOptions optionsOrNull) {
		Assert.notNull(entities);
		Assert.notNull(tableName);
		doBatchInsert(asychronously, tableName, entities, optionsOrNull);
	}

	@Override
	public <T> void saveNew(boolean asychronously, T entity, QueryOptions optionsOrNull) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		saveNew(asychronously, entity, tableName, optionsOrNull);
	}

	@Override
	public <T> void saveNew(boolean asychronously, T entity, String tableName, QueryOptions optionsOrNull) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		Assert.notNull(tableName);
		assertNotIterable(entity);
		doInsert(asychronously, tableName, entity, optionsOrNull);
	}

	@Override
	public <T> List<T> findByQuery(Select cql, Class<T> selectClass) {
		return findByQuery(cql.getQueryString(), selectClass);
	}

	@Override
	public <T> List<T> findByQuery(String cql, Class<T> selectClass) {
		return doSelect(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	@Override
	public <T> T findOneByQuery(Select selectQuery, Class<T> selectClass) {
		return findOneByQuery(selectQuery.getQueryString(), selectClass);
	}

	@Override
	public <T> T findOneByQuery(String cql, Class<T> selectClass) {
		return doSelectOne(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	@Override
	public <T> void saveInBatch(boolean asychronously, Iterable<T> entities, QueryOptions optionsOrNull) {
		Assert.notNull(entities);
		doBatchUpdate(asychronously, null, entities, optionsOrNull);
	}

	@Override
	public <T> void saveInBatch(boolean asychronously, Iterable<T> entities, String tableName, QueryOptions optionsOrNull) {
		Assert.notNull(entities);
		Assert.notNull(tableName);
		doBatchUpdate(asychronously, tableName, entities, optionsOrNull);
	}

	@Override
	public <T> void save(boolean asychronously, T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		save(asychronously, entity, tableName, options);
	}

	@Override
	public <T> void save(boolean asychronously, T entity, String tableName, QueryOptions optionsOrNull) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		Assert.notNull(tableName);
		doUpdate(asychronously, tableName, entity, optionsOrNull);
	}

	/**
	 * @param obj
	 * @return
	 */
	private <T> String determineTableName(T obj) {
		if (null != obj) {
			return determineTableName(obj.getClass());
		}

		return null;
	}

	/**
	 * @param query
	 * @param readRowCallback
	 * @return
	 */
	private <T> List<T> doSelect(final String query, ReadRowCallback<T> readRowCallback) {

		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		List<T> result = new ArrayList<T>();
		Iterator<Row> iterator = resultSet.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			result.add(readRowCallback.doWith(row));
		}

		return result;
	}

	/**
	 * @param selectQuery
	 * @return
	 */
	private Long doSelectCount(final Select query) {

		Long count = null;

		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		Iterator<Row> iterator = resultSet.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			count = row.getLong(0);
		}

		return count;

	}

	/**
	 * @param query
	 * @param readRowCallback
	 * @return
	 */
	private <T> T doSelectOne(final String query, ReadRowCallback<T> readRowCallback) {

		logger.info(query);

		/*
		 * Run the Query
		 */
		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		Iterator<Row> iterator = resultSet.iterator();
		if (iterator.hasNext()) {
			Row row = iterator.next();
			T result = readRowCallback.doWith(row);
			if (iterator.hasNext()) {
				throw new DuplicateKeyException("found two or more results in query " + query);
			}
			return result;
		}

		return null;
	}

	/**
	 * Insert a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entities
	 * @param optionsByName
	 * @param insertAsychronously
	 * @return
	 */
	protected <T> void doBatchInsert(final boolean insertAsychronously, String tableNameOrNull,
			final Iterable<T> entities, QueryOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T objectToSave : entities) {

			Assert.notNull(objectToSave);
			assertNotIterable(objectToSave);

			if (tableNameOrNull == null) {
				tableNameOrNull = getTableName(objectToSave.getClass());
				Assert.notNull(tableNameOrNull);
			}

			batch.add((Statement) toInsertQuery(tableNameOrNull, objectToSave, optionsOrNull));
			emptyBatch = false;
		}

		if (emptyBatch) {
			throw new IllegalArgumentException("entities are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		logger.info(batch.getQueryString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public List<T> doInSession(Session s) {

				if (insertAsychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

	}

	/**
	 * Update a Batch of rows in a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entities
	 * @param optionsByName
	 * @param updateAsychronously
	 * @return
	 */
	protected <T> void doBatchUpdate(final boolean updateAsychronously, String tableNameOrNull,
			final Iterable<T> entities, QueryOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T objectToSave : entities) {

			Assert.notNull(objectToSave);
			assertNotIterable(objectToSave);

			if (tableNameOrNull == null) {
				tableNameOrNull = getTableName(objectToSave.getClass());
				Assert.notNull(tableNameOrNull);
			}

			batch.add((Statement) toUpdateQuery(tableNameOrNull, objectToSave, optionsOrNull));
			emptyBatch = false;
		}

		if (emptyBatch) {
			throw new IllegalArgumentException("entities are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		logger.info(batch.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public List<T> doInSession(Session s) {

				if (updateAsychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

	}

	/**
	 * Perform the removal of a Row.
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doDeleteById(final boolean asychronously, final String tableName, final T id,
			Class<?> entityClass, QueryOptions optionsOrNull) {

		final Query query = toDeleteQueryById(tableName, id, entityClass, optionsOrNull);

		logger.info(query.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

				if (asychronously) {
					s.executeAsync(query);
				} else {
					s.execute(query);
				}

				return null;

			}
		});

	}

	/**
	 * Perform the removal of a Row.
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doDelete(final boolean asychronously, final String tableName, final T objectToRemove,
			QueryOptions optionsOrNull) {

		final Query query = toDeleteQuery(tableName, objectToRemove, optionsOrNull);

		logger.info(query.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

				if (asychronously) {
					s.executeAsync(query);
				} else {
					s.execute(query);
				}

				return null;

			}
		});

	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doExecute(SessionCallback<T> callback) {

		Assert.notNull(callback);

		try {

			return callback.doInSession(getSession());

		} catch (RuntimeException e) {
			throw translateIfPossible(e);
		}
	}

	/**
	 * Insert a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entity
	 */
	protected <T> void doInsert(final boolean insertAsychronously, final String tableName, final T entity,
			QueryOptions optionsOrNull) {

		final Query query = toInsertQuery(tableName, entity, optionsOrNull);

		logger.info(query.toString());
		if (query.getConsistencyLevel() != null) {
			logger.info(query.getConsistencyLevel().name());
		}
		if (query.getRetryPolicy() != null) {
			logger.info(query.getRetryPolicy().toString());
		}

		doExecute(new SessionCallback<Object>() {

			@Override
			public T doInSession(Session s) {

				if (insertAsychronously) {
					s.executeAsync(query);
				} else {
					s.execute(query);
				}

				return null;

			}
		});

	}

	/**
	 * Update a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entity
	 * @param optionsByName
	 * @param updateAsychronously
	 * @return
	 */
	protected <T> void doUpdate(final boolean updateAsychronously, final String tableName, final T entity,
			QueryOptions optionsOrNull) {

		final Query q = toUpdateQuery(tableName, entity, optionsOrNull);

		logger.info(q.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public T doInSession(Session s) {

				if (updateAsychronously) {
					s.executeAsync(q);
				} else {
					s.execute(q);
				}

				return null;

			}
		});

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
	 * Generates a Query Object for an insert
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public Query toInsertQuery(String tableName, final Object objectToSave, QueryOptions optionsOrNull) {

		final Insert query = QueryBuilder.insertInto(keyspace, tableName);

		/*
		 * Write properties
		 */
		cassandraConverter.write(objectToSave, query);

		/*
		 * Add Query Options
		 */
		addQueryOptions(query, optionsOrNull);

		/*
		 * Add TTL to Insert object
		 */
		addInsertOptions(query, optionsOrNull);

		return query;

	}

	/**
	 * Generates a Query Object for an Update
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public Query toUpdateQuery(String tableName, final Object objectToSave, QueryOptions optionsOrNull) {

		final Update query = QueryBuilder.update(keyspace, tableName);

		/*
		 * Write properties
		 */
		cassandraConverter.write(objectToSave, query);

		/*
		 * Add Query Options
		 */
		addQueryOptions(query, optionsOrNull);

		/*
		 * Add TTL to Insert object
		 */
		addUpdateOptions(query, optionsOrNull);

		return query;

	}

	/**
	 * Create a Delete Query Object from an annotated POJO
	 * 
	 * @param keyspace
	 * @param tableName
	 * @param id
	 * @param entity
	 * @param optionsByName
	 * @return
	 */
	public Query toDeleteQueryById(String tableName, final Object id, Class<?> entityClass, QueryOptions optionsOrNull) {

		CassandraPersistentEntity<?> entity = getEntity(entityClass);

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete query = ds.from(keyspace, tableName);
		final Where w = query.where();

		List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

		for (Clause c : list) {
			w.and(c);
		}

		addQueryOptions(query, optionsOrNull);

		addDeleteOptions(query, optionsOrNull);

		return query;

	}

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDeleteById(final boolean asychronously, String tableName, final Iterable<T> ids,
			Class<?> entityClass, QueryOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T id : ids) {

			Assert.notNull(id);
			assertNotIterable(id);

			batch.add((Statement) toDeleteQueryById(tableName, id, entityClass, optionsOrNull));
			emptyBatch = false;

		}

		if (emptyBatch) {
			throw new IllegalArgumentException("ids are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		logger.info(batch.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

				if (asychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

	}

	/**
	 * Create a Delete Query Object from an annotated POJO
	 * 
	 * @param keyspace
	 * @param tableName
	 * @param objectToRemove
	 * @param entity
	 * @param optionsByName
	 * @return
	 */
	public Query toDeleteQuery(String tableName, final Object objectToRemove, QueryOptions optionsOrNull) {

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete query = ds.from(keyspace, tableName);
		final Where w = query.where();

		/*
		 * Write where condition to find by Id
		 */
		cassandraConverter.write(objectToRemove, w);

		addQueryOptions(query, optionsOrNull);

		addDeleteOptions(query, optionsOrNull);

		return query;

	}

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDelete(final boolean asychronously, String tableNameOrNull, final Iterable<T> entities,
			QueryOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T objectToSave : entities) {

			Assert.notNull(objectToSave);
			assertNotIterable(objectToSave);

			if (tableNameOrNull == null) {
				tableNameOrNull = getTableName(objectToSave.getClass());
				Assert.notNull(tableNameOrNull);
			}

			batch.add((Statement) toDeleteQuery(tableNameOrNull, objectToSave, optionsOrNull));
			emptyBatch = false;

		}

		if (emptyBatch) {
			throw new IllegalArgumentException("entities are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		logger.info(batch.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

				if (asychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

	}

	protected <T> T doFindById(Object id, Class<T> entityClass, String tableName, QueryOptions optionsOrNull) {
		Select select = QueryBuilder.select().all().from(tableName);
		Select.Where w = select.where();

		CassandraPersistentEntity<?> entity = getEntity(entityClass);

		List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

		for (Clause c : list) {
			w.and(c);
		}

		addQueryOptions(select, optionsOrNull);

		return doSelectOne(select.getQueryString(), new ReadRowCallback<T>(cassandraConverter, entityClass));

	}

	/**
	 * Service method for persistent entity lookup
	 * 
	 * @param entityClass
	 * @return CassandraPertistentEntity
	 */

	protected CassandraPersistentEntity<?> getEntity(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("persistent entity not found for a given class " + entityClass);
		}

		return entity;
	}

	@Override
	public <T> List<T> findByPartitionKey(Object id, Class<T> entityClass, QueryOptions options) {
		return doFindByPartitionKey(id, entityClass, getTableName(entityClass), options);
	}

	@Override
	public <T> List<T> findByPartitionKey(Object id, Class<T> entityClass, String tableName, QueryOptions optionsOrNull) {
		return doFindByPartitionKey(id, entityClass, tableName, optionsOrNull);
	}

	protected <T> List<T> doFindByPartitionKey(Object id, Class<T> entityClass, String tableName,
			QueryOptions optionsOrNull) {

		Select select = QueryBuilder.select().all().from(tableName);
		Select.Where w = select.where();

		CassandraPersistentEntity<?> entity = getEntity(entityClass);

		List<Clause> list = cassandraConverter.getPartitionKey(entity, id);

		for (Clause c : list) {
			w.and(c);
		}

		addQueryOptions(select, optionsOrNull);

		return doSelect(select.getQueryString(), new ReadRowCallback<T>(cassandraConverter, entityClass));
	}
}
