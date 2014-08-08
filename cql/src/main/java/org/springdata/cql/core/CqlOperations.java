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
package org.springdata.cql.core;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

/**
 * Operations for interacting with Cassandra at the lowest level. This interface provides Exception Translation.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 */
public interface CqlOperations {

	/**
	 * Creates query by using QueryCreator
	 * 
	 * @param qc - QueryCreator interface
	 * @return
	 */
	Statement createQuery(QueryCreator qc);

	/**
	 * Executes the supplied {@link SessionCallback} in the current Template Session. The implementation of
	 * SessionCallback can decide whether or not to <code>execute()</code> or <code>executeAsync()</code> the operation.
	 * 
	 * @param sessionCallback
	 * @return Type<T> defined in the SessionCallback
	 */
	<T> T execute(SessionCallback<T> sessionCallback);

	/**
	 * Executes the specific query
	 * 
	 * @param query
	 * @return ResultSet
	 */

	ResultSet execute(Statement query);

	/**
	 * Executes the supplied CQL Query and returns ResultSet
	 * 
	 * @param cql The CQL String
	 */
	ResultSet execute(String cql);

	/**
	 * Builds ExecuteOperation for the supplied CQL Query
	 * 
	 * @param cql The CQL String
	 */
	ExecuteOperation buildExecuteOperation(String cql);

	/**
	 * Builds ExecuteOperation for the supplied Query
	 * 
	 * @param cql The CQL String
	 */
	ExecuteOperation buildExecuteOperation(Statement query);

	/**
	 * Executes the supplied PreparedStatement with custom binder
	 * 
	 * @param ps PreparedStatement
	 * @param psb PreparedStatementBinder if exists
	 */
	ResultSet execute(PreparedStatement ps, PreparedStatementBinder psb);

	/**
	 * Builds ExecuteOperation for the supplied PreparedStatement with custom binder
	 * 
	 * @param ps PreparedStatement
	 * @param psb PreparedStatementBinder if exists
	 */
	ExecuteOperation buildExecuteOperation(PreparedStatement ps, PreparedStatementBinder psb);

	/**
	 * Executes the supplied PreparedStatement with custom binder
	 * 
	 * @param bs BoundStatement
	 */
	ResultSet execute(BoundStatement bs);

	/**
	 * Builds ExecuteOperation for the supplied PreparedStatement with custom binder
	 * 
	 * @param bs BoundStatement
	 */
	ExecuteOperation buildExecuteOperation(BoundStatement bs);

	/**
	 * Executes the supplied CQL Query and returns ResultSet
	 * 
	 * @param qc The QueryCreator
	 */
	ResultSet execute(QueryCreator qc);

	/**
	 * Builds ExecuteOperation for the supplied CQL Query
	 * 
	 * @param qc The QueryCreator
	 */
	ExecuteOperation buildExecuteOperation(QueryCreator qc);

	/**
	 * Executes the supplied CQL Query batch and returns ResultSet
	 * 
	 * @param sqls The CQL queries
	 */
	ResultSet executeInBatch(String[] cqls);

	/**
	 * Builds ExecuteOperation for the supplied CQL Query batch
	 * 
	 * @param sqls The CQL queries
	 */
	ExecuteOperation buildExecuteInBatchOperation(String[] cqls);

	/**
	 * Executes the supplied CQL Query batch and returns ResultSet
	 * 
	 * @param statements The Statements
	 */
	ResultSet executeInBatch(Iterable<RegularStatement> statements);

	/**
	 * Builds ExecuteOperation for the supplied CQL Query batch
	 * 
	 * @param statements The Statements
	 */
	ExecuteOperation buildExecuteInBatchOperation(Iterable<RegularStatement> statements);

	/**
	 * Builds SelectOperation for the provided CQL Query
	 * 
	 * @param cql The CQL Query String
	 * 
	 * @return SelectOperation
	 */
	SelectOperation buildSelectOperation(String cql);

	/**
	 * Builds SelectOperation for the provided prepared statement
	 * 
	 * @param ps PreparedStatement
	 * @param psb PreparedStatementBinder if exists
	 * 
	 * @return SelectOperation
	 */
	SelectOperation buildSelectOperation(PreparedStatement ps, PreparedStatementBinder psb);

	/**
	 * Builds SelectOperation for the provided BoundStatement
	 * 
	 * @param bs BoundStatement
	 * 
	 * @return SelectOperation
	 */
	SelectOperation buildSelectOperation(BoundStatement bs);

	/**
	 * Builds SelectOperation for the provided QueryCreator
	 * 
	 * @param qc The QueryCreator
	 * 
	 * @return SelectOperation
	 */
	SelectOperation buildSelectOperation(QueryCreator qc);

	/**
	 * Processes the ResultSet through the RowCallbackHandler and return nothing. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rch RowCallbackHandler with the processing implementation
	 */
	void process(ResultSet resultSet, RowCallbackHandler rch);

	/**
	 * Processes the ResultSet through the RowMapper and returns the List of mapped Rows. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rowMapper RowMapper with the processing implementation
	 * @return List of <T> generated by the RowMapper
	 */
	<T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper);

	/**
	 * Process a ResultSet through a RowMapper. This is used internal to the Template for core operations, but is made
	 * available through Operations in the event you have a ResultSet to process. The ResultsSet could come from a
	 * ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param rowMapper
	 * @param singleResult Expected single result
	 * @return
	 */
	<T> T processOne(ResultSet resultSet, RowMapper<T> rowMapper, boolean singleResult);

	/**
	 * Process a ResultSet, trying to convert the first columns of the first Row to Class<T>. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param elementType
	 * @param singleResult Expected single result
	 * @return
	 */
	<T> T processOneFirstColumn(ResultSet resultSet, Class<T> elementType, boolean singleResult);

	/**
	 * Process a ResultSet with <b>ONE</b> Row and convert to a Map. This is used internal to the Template for core
	 * operations, but is made available through Operations in the event you have a ResultSet to process. The ResultsSet
	 * could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param singleResult Expected single result
	 * @return
	 */
	Map<String, Object> processOneAsMap(ResultSet resultSet, boolean singleResult);

	/**
	 * Process a ResultSet and convert the first column of the results to a List. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param elementType
	 * @return
	 */
	<T> List<T> processFirstColumn(ResultSet resultSet, Class<T> elementType);

	/**
	 * Process a ResultSet and convert it to a List of Maps with column/value. This is used internal to the Template for
	 * core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @return
	 */
	List<Map<String, Object>> processAsMap(ResultSet resultSet);

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b>
	 * 
	 * @param cql The CQL Statement to prepare
	 * @return PreparedStatement
	 */
	PreparedStatement prepareStatement(String cql);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new PreparedSession
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @return PreparedStatement
	 */
	PreparedStatement prepareStatement(PreparedStatementCreator psc);

	/**
	 * Executes the prepared statement and processes the statement using the provided Callback. <b>This can only be used
	 * for CQL Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * PreparedStatementCallback implementation provided by the Application Code.
	 * 
	 * @param ps The implementation to create the PreparedStatement
	 * @param psc What to do with the results of the PreparedStatement
	 * @return Type<T> as determined by the supplied Callback.
	 */
	<T> T execute(PreparedStatement ps, PreparedStatementCallback<T> psc);

	/**
	 * Binds prepared statement
	 * 
	 * @param ps The PreparedStatement
	 * @return
	 */
	BoundStatement bind(PreparedStatement ps);

	/**
	 * Binds prepared statement
	 * 
	 * @param ps The PreparedStatement
	 * @param psb The implementation to bind variables to values if exists
	 * @return
	 */
	BoundStatement bind(PreparedStatement ps, PreparedStatementBinder psb);

	/**
	 * Describe the current Ring. This uses the provided {@link RingMemberHostMapper} to provide the basics of the
	 * Cassandra Ring topology.
	 * 
	 * @return The collection of ring tokens that are active in the cluster
	 */
	List<RingMember> describeRing();

	/**
	 * Describe the current Ring. Application code must provide its own {@link HostMapper} implementation to process the
	 * lists of hosts returned by the Cassandra Cluster Metadata.
	 * 
	 * @param hostMapper The implementation to use for host mapping.
	 * @return Collection generated by the provided HostMapper.
	 */
	<T> List<T> describeRing(HostMapper<T> hostMapper);

	/**
	 * Get the current Session used for operations in the implementing class.
	 * 
	 * @return The DataStax Driver Session Object
	 */
	Session getSession();

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * This is used internally by the other ingest() methods, but can be used if you want to write your own RowIterator.
	 * The Object[] length returned by the next() implementation must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param asynchronously Do asynchronously or not
	 * @param ps The PreparedStatement
	 * @param rows Implementation to provide the Object[] to be bound to the CQL.
	 */
	List<ResultSet> ingest(PreparedStatement ps, Iterable<Object[]> rows);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * This is used internally by the other ingest() methods, but can be used if you want to write your own RowIterator.
	 * The Object[] length returned by the next() implementation must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param asynchronously Do asynchronously or not
	 * @param ps The PreparedStatement
	 * @param rows Implementation to provide the Object[] to be bound to the CQL.
	 */
	IngestOperation buildIngestOperation(PreparedStatement ps, Iterable<Object[]> rows);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The Object[] length of the nested array must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param asynchronously Do asynchronously or not
	 * @param ps The PreparedStatement
	 * @param rows Object array of Object array of values to bind to the CQL.
	 */
	List<ResultSet> ingest(PreparedStatement ps, Object[][] rows);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The Object[] length of the nested array must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param asynchronously Do asynchronously or not
	 * @param ps The PreparedStatement
	 * @param rows Object array of Object array of values to bind to the CQL.
	 */
	IngestOperation buildIngestOperation(PreparedStatement ps, Object[][] rows);

	/**
	 * Calculates number of rows in table
	 * 
	 * @param tableName
	 * @return
	 */
	Long countAll(String tableName);

	/**
	 * Calculates number of rows in table
	 * 
	 * @param tableName
	 * @return
	 */
	ProcessOperation<Long> buildCountAllOperation(String tableName);

	/**
	 * Delete all rows in the table
	 * 
	 * @param tableName
	 */
	ResultSet truncate(String tableName);

	/**
	 * Delete all rows in the table
	 * 
	 * @param tableName
	 */
	ExecuteOperation buildTruncateOperation(String tableName);

	/**
	 * Support admin operations
	 * 
	 * @return CassandraAdminOperations
	 */

	AdminCqlOperations getAdminOperations();

	/**
	 * Support schema operations
	 * 
	 * @return CassandraSchemaOperations
	 */

	SchemaCqlOperations getSchemaOperations();

}
