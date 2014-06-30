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

import java.util.List;

import org.springdata.cassandra.convert.CassandraConverter;
import org.springdata.cassandra.cql.core.CqlOperations;
import org.springdata.cassandra.cql.core.ResultSetExtractor;
import org.springdata.cassandra.cql.core.RowMapper;

import com.datastax.driver.core.ResultSet;

/**
 * Operations for interacting with Cassandra. These operations are also used by the SimpleCassandraRepository
 * implementation.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 * 
 */
public interface CassandraOperations {

	/**
	 * The table name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getTableName(Class<?> entityClass);

	/**
	 * Finds all entities in table
	 * 
	 * @param entityClass
	 * @return List<T>
	 */
	<T> List<T> findAll(Class<T> entityClass);

	/**
	 * Finds all entities in table
	 * 
	 * @param entityClass
	 * @return GetOperation
	 */
	<T> GetOperation<List<T>> getFindAllOperation(Class<T> entityClass);

	/**
	 * Finds all entities with specific ids in table
	 * 
	 * @param entityClass
	 * @param ids
	 * @return List<T>
	 */
	<T> List<T> findAll(Class<T> entityClass, Iterable<?> ids);

	/**
	 * Finds all entities with specific ids in table
	 * 
	 * @param entityClass
	 * @param ids
	 * @return GetOperation
	 */
	<T> GetOperation<List<T>> getFindAllOperation(Class<T> entityClass, Iterable<?> ids);

	/**
	 * Finds entity by id
	 * 
	 * @param entityClass
	 * @param id
	 * @return T
	 */
	<T> T findById(Class<T> entityClass, Object id);

	/**
	 * Finds entity by id
	 * 
	 * @param entityClass
	 * @param id
	 * @return GetOperation
	 */
	<T> GetOperation<T> getFindByIdOperation(Class<T> entityClass, Object id);

	/**
	 * Finds a list of instances with the specified partition part of the primary key
	 * 
	 * @param entityClass
	 * @param id
	 * @param <T>
	 * @return List<T>
	 */
	<T> List<T> findByPartitionKey(Class<T> entityClass, Object id);

	/**
	 * Finds a list of instances with the specified partition part of the primary key
	 * 
	 * @param entityClass
	 * @param id
	 * @param <T>
	 * @return GetOperation
	 */
	<T> GetOperation<List<T>> getFindByPartitionKeyOperation(Class<T> entityClass, Object id);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param entityClass must not be {@literal null}, mapped entity type.
	 * @param cql must not be {@literal null}.
	 * @return List<T>
	 */
	<T> List<T> find(Class<T> entityClass, String cql);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param entityClass must not be {@literal null}, mapped entity type.
	 * @param cql must not be {@literal null}.
	 * @return GetOperation
	 */
	<T> GetOperation<List<T>> getFindOperation(Class<T> entityClass, String cql);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param entityClass must not be {@literal null}, mapped entity type.
	 * @param cql must not be {@literal null}.
	 * @return T
	 */
	<T> T findOne(Class<T> entityClass, String cql);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param entityClass must not be {@literal null}, mapped entity type.
	 * @param cql must not be {@literal null}.
	 * @return
	 */
	<T> GetOperation<T> getFindOneOperation(Class<T> entityClass, String cql);

	/**
	 * Counts rows for given entity
	 * 
	 * @param entityClass
	 * @return Long
	 */
	<T> Long countAll(Class<T> entityClass);

	/**
	 * Counts rows for given entity
	 * 
	 * @param entityClass
	 * @return GetOperation
	 */
	<T> GetOperation<Long> getCountAllOperation(Class<T> entityClass);

	/**
	 * Checks if entity exists in Cassandra
	 * 
	 * @param entity
	 * @return Boolean
	 */
	<T> Boolean exists(T entity);

	/**
	 * Checks if entity exists in Cassandra
	 * 
	 * @param entity
	 * @return GetOperation
	 */
	<T> GetOperation<Boolean> getExistsOperation(T entity);

	/**
	 * Checks if entity exists in Cassandra
	 * 
	 * @param entityClass
	 * @param id
	 * @return Boolean
	 */
	<T> Boolean exists(Class<T> entityClass, Object id);

	/**
	 * Checks if entity exists in Cassandra
	 * 
	 * @param entityClass
	 * @param id
	 * @return GetOperation
	 */
	<T> GetOperation<Boolean> getExistsOperation(Class<T> entityClass, Object id);

	/**
	 * Insert the given object to the table.
	 * 
	 * @param entity
	 */
	<T> ResultSet saveNew(T entity);

	/**
	 * Insert the given object to the table.
	 * 
	 * @param entity
	 */
	<T> SaveNewOperation getSaveNewOperation(T entity);

	/**
	 * Insert the given list of objects to the table.
	 * 
	 * @param entities
	 * @return ResultSet
	 */
	<T> ResultSet saveNewInBatch(Iterable<T> entities);

	/**
	 * Insert the given list of objects to the table.
	 * 
	 * @param entities
	 * @return BatchOperation
	 */
	<T> BatchOperation getSaveNewInBatchOperation(Iterable<T> entities);

	/**
	 * Updates the given object in the table.
	 * 
	 * @param entity to save
	 * @return ResultSet
	 */
	<T> ResultSet save(T entity);

	/**
	 * Updates the given object in the table.
	 * 
	 * @param entity to save
	 * @return SaveOperation
	 */
	<T> SaveOperation getSaveOperation(T entity);

	/**
	 * Updates list of objects in the table.
	 * 
	 * @param entities
	 * @return ResultSet
	 */
	<T> ResultSet saveInBatch(Iterable<T> entities);

	/**
	 * Updates list of objects in the table.
	 * 
	 * @param entities
	 * @return BatchOperation
	 */
	<T> BatchOperation getSaveInBatchOperation(Iterable<T> entities);

	/**
	 * Removes the given object by id from the given table.
	 * 
	 * @param entityClass
	 * @param id
	 */
	<T> ResultSet deleteById(Class<T> entityClass, Object id);

	/**
	 * Removes the given object by id from the given table.
	 * 
	 * @param entityClass
	 * @param id
	 */
	<T> DeleteOperation getDeleteByIdOperation(Class<T> entityClass, Object id);

	/**
	 * Remove list of objects from the table by given ids.
	 * 
	 * @param entityClass
	 * @param ids
	 */
	<T> ResultSet deleteByIdInBatch(Class<T> entityClass, Iterable<?> ids);

	/**
	 * Remove list of objects from the table by given ids.
	 * 
	 * @param entityClass
	 * @param ids
	 */
	<T> BatchOperation getDeleteByIdInBatchOperation(Class<T> entityClass, Iterable<?> ids);

	/**
	 * Remove entity from the table
	 * 
	 * @param entity
	 */
	<T> ResultSet delete(T entity);

	/**
	 * Remove entity from the table
	 * 
	 * @param entity
	 */
	<T> DeleteOperation getDeleteOperation(T entity);

	/**
	 * Delete entities in batch operation
	 * 
	 * @param entities
	 */
	<T> ResultSet deleteInBatch(Iterable<T> entities);

	/**
	 * Delete entities in batch operation
	 * 
	 * @param entities
	 */
	<T> BatchOperation getDeleteInBatchOperation(Iterable<T> entities);

	/**
	 * Delete all entities in table
	 * 
	 * @param entityClass
	 * @return
	 */
	<T> ResultSet deleteAll(Class<T> entityClass);

	/**
	 * Delete all entities in table
	 * 
	 * @param entityClass
	 * @return
	 */
	<T> DeleteOperation getDeleteAllOperation(Class<T> entityClass);

	/**
	 * Returns RowMapper based on Cassandra Converter.
	 * 
	 * @param entityClass Entity class used to convert ResultSet to type T
	 * @return RowMapper that can be used in SelectOperation
	 */

	<T> RowMapper<T> getRowMapperFor(Class<T> entityClass);

	/**
	 * Returns ResultSetExtractor based on Cassandra Converter.
	 * 
	 * @param entityClass Entity class used to convert ResultSet to type T
	 * @return ResultSetExtractor that can be used in SelectOperation
	 */

	<T> ResultSetExtractor<List<T>> getResultSetExtractorFor(Class<T> entityClass);

	/**
	 * Processes the ResultSet through the CassandraConverter and returns the List of mapped Rows. This is used internal
	 * to the Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param entityClass Entity class used to mapping results
	 * @return List of <T>
	 */
	<T> List<T> process(ResultSet resultSet, Class<T> entityClass);

	/**
	 * Processes the ResultSet through the RowCallbackHandler and return nothing. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param entityClass Entity class used to mapping results
	 * @param ech EntryCallbackHandler with the processing implementation
	 */
	<T> void process(ResultSet resultSet, Class<T> entityClass, EntryCallbackHandler<T> ech);

	/**
	 * Process a ResultSet through the CassandraConverter. This is used internal to the Template for core operations, but
	 * is made available through Operations in the event you have a ResultSet to process. The ResultsSet could come from a
	 * ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param entityClass
	 * @param singleResult Expected single result in ResultSet
	 * @return
	 */
	<T> T processOne(ResultSet resultSet, Class<T> entityClass, boolean singleResult);

	/**
	 * Returns the underlying keyspace.
	 * 
	 * @return
	 */
	String getKeyspace();

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
	CassandraConverter getConverter();

	/**
	 * Returns table specific operations
	 * 
	 * @return TableDataOperations
	 */
	SchemaOperations schemaOps();

	/**
	 * Returns Cql specific operations
	 * 
	 * @return CqlOperations
	 */
	CqlOperations cqlOps();

}
