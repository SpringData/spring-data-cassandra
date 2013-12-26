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

import java.util.List;

import org.springdata.cassandra.base.core.query.QueryOptions;
import org.springdata.cassandra.data.convert.CassandraConverter;

import com.datastax.driver.core.querybuilder.Select;

/**
 * Operations for interacting with Cassandra. These operations are used by the Repository implementation, but can also
 * be used directly when that is desired by the developer.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 * 
 */
public interface CassandraDataOperations {

	/**
	 * The table name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getTableName(Class<?> entityClass);

	/**
	 * 
	 * @param id
	 * @param entityClass
	 * @param optionsOrNull
	 * @return
	 */

	<T> T findById(Object id, Class<T> entityClass, QueryOptions optionsOrNull);

	/**
	 * 
	 * @param id
	 * @param entityClass
	 * @param tableName
	 * @param optionsOrNull
	 * @return
	 */

	<T> T findById(Object id, Class<T> entityClass, String tableName, QueryOptions optionsOrNull);

	/**
	 * Finds a list of instances with the specified partition part of the primary key
	 * 
	 * @param id
	 * @param entityClass
	 * @param optionsOrNull
	 * @param <T>
	 * @return
	 */
	<T> List<T> findByPartitionKey(Object id, Class<T> entityClass, QueryOptions optionsOrNull);

	/**
	 * Finds a list of instances with the specified partition part of the primary key
	 * 
	 * @param id
	 * @param entityClass
	 * @param tableName
	 * @param options
	 * @param <T>
	 * @return
	 */
	<T> List<T> findByPartitionKey(Object id, Class<T> entityClass, String tableName, QueryOptions options);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param query must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */
	@Deprecated
	<T> List<T> findByQuery(String cql, Class<T> selectClass);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param selectQuery must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */
	@Deprecated
	<T> List<T> findByQuery(Select selectQuery, Class<T> selectClass);

	/**
	 * Execute query and convert ResultSet to the entity
	 * 
	 * @param query must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */
	@Deprecated
	<T> T findOneByQuery(String cql, Class<T> selectClass);

	@Deprecated
	<T> T findOneByQuery(Select selectQuery, Class<T> selectClass);

	/**
	 * Counts rows for given query
	 * 
	 * @param selectQuery
	 * @return
	 */
	@Deprecated
	Long countByQuery(Select selectQuery);

	/**
	 * Counts all rows for given table
	 * 
	 * @param tableName
	 * @return
	 */

	Long count(String tableName);

	/**
	 * Insert the given object to the table.
	 * 
	 * @param entity
	 * @param optionsOrNull
	 */
	<T> void saveNew(boolean asychronously, T entity, QueryOptions optionsOrNull);

	/**
	 * Insert the given object to the table.
	 * 
	 * @param entity
	 * @param tableName
	 * @param optionsOrNull
	 * @return
	 */
	<T> void saveNew(boolean asychronously, T entity, String tableName, QueryOptions optionsOrNull);

	/**
	 * Insert the given list of objects to the table.
	 * 
	 * @param entities
	 * @param optionsOrNull
	 * @return
	 */
	<T> void saveNewInBatch(boolean asychronously, Iterable<T> entities, QueryOptions optionsOrNull);

	/**
	 * Insert the given list of objects to the table.
	 * 
	 * @param entities
	 * @param tableName
	 * @param optionsOrNull
	 * @return
	 */
	<T> void saveNewInBatch(boolean asychronously, Iterable<T> entities, String tableName, QueryOptions optionsOrNull);

	/**
	 * Updates the given object in the table.
	 * 
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> void save(boolean asychronously, T entity, QueryOptions optionsOrNull);

	/**
	 * Updates the given object in the table.
	 * 
	 * @param entity
	 * @param tableName
	 * @param optionsOrNull
	 * @return
	 */
	<T> void save(boolean asychronously, T entity, String tableName, QueryOptions optionsOrNull);

	/**
	 * Updates list of objects in the table.
	 * 
	 * @param entities
	 * @param tableName
	 * @param optionsOrNull
	 * @return
	 */
	<T> void saveInBatch(boolean asychronously, Iterable<T> entities, QueryOptions optionsOrNull);

	/**
	 * Updates list of objects in the table.
	 * 
	 * @param entities
	 * @param tableName
	 * @param optionsOrNull
	 * @return
	 */
	<T> void saveInBatch(boolean asychronously, Iterable<T> entities, String tableName, QueryOptions optionsOrNull);

	/**
	 * Removes the given object by id from the given table.
	 * 
	 * @param id
	 * @param entityClass
	 * @param optionsOrNull
	 */
	<T> void deleteById(boolean asychronously, T id, Class<?> entityClass, QueryOptions optionsOrNull);

	/**
	 * Removes the given object by id from the given table.
	 * 
	 * @param id
	 * @param entityClass
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void deleteById(boolean asychronously, T id, Class<?> entityClass, String tableName, QueryOptions optionsOrNull);

	/**
	 * Remove list of objects from the table by given ids.
	 * 
	 * @param ids
	 * @param entityClass
	 * @param optionsOrNull
	 */
	<T> void deleteInBatchById(boolean asychronously, Iterable<T> ids, Class<?> entityClass, QueryOptions optionsOrNull);

	/**
	 * Remove list of objects from the table by given ids.
	 * 
	 * @param ids
	 * @param entityClass
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void deleteInBatchById(boolean asychronously, Iterable<T> ids, Class<?> entityClass, String tableName,
			QueryOptions optionsOrNull);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void delete(boolean asychronously, T entity, QueryOptions optionsOrNull);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void delete(boolean asychronously, T entity, String tableName, QueryOptions optionsOrNull);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void deleteInBatch(boolean asychronously, Iterable<T> entities, QueryOptions optionsOrNull);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void deleteInBatch(boolean asychronously, Iterable<T> entities, String tableName, QueryOptions optionsOrNull);

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
	CassandraConverter getConverter();
}
