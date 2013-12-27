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
package org.springdata.cassandra.base.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springdata.cassandra.base.core.query.QueryOptions;
import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Operations for interacting with Cassandra at the lowest level. This interface provides Exception Translation.
 * 
 * @author David Webb
 * @author Matthew Adams
 * @author Alex Shvid
 */
public interface CassandraOperations {

	/**
	 * Executes the supplied {@link SessionCallback} in the current Template Session. The implementation of
	 * SessionCallback can decide whether or not to <code>execute()</code> or <code>executeAsync()</code> the operation.
	 * 
	 * @param sessionCallback
	 * @return Type<T> defined in the SessionCallback
	 */
	<T> T execute(SessionCallback<T> sessionCallback);

	/**
	 * Executes the supplied CQL Query and returns nothing.
	 * 
	 * @param asynchronously Flag to execute asynchronously
	 * @param cql The Query
	 * @param optionsOrNull Query Options Object if exists
	 */
	void execute(boolean asynchronously, String cql, QueryOptions optionsOrNull);

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * @param optionsOrNull Query Options Object if exists
	 * 
	 * @return
	 * @throws DataAccessException
	 */
	<T> T query(String cql, ResultSetExtractor<T> rse, QueryOptions optionsOrNull);

	/**
	 * Executes the provided CQL Query asynchronously, and extracts the results with the ResultSetFutureExtractor
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the future results
	 * @param optionsOrNull Query Options Object if exists
	 * @return
	 * @throws DataAccessException
	 */
	<T> T queryAsynchronously(String cql, ResultSetFutureExtractor<T> rse, QueryOptions optionsOrNull);

	/**
	 * Executes the provided CQL Query, and then processes the results with the <code>RowCallbackHandler</code>.
	 * 
	 * @param cql The Query
	 * @param rch The implementation for processing the rows returned.
	 * @param optionsOrNull Query Options Object
	 * @throws DataAccessException
	 */
	void query(String cql, RowCallbackHandler rch, QueryOptions optionsOrNull);

	/**
	 * Processes the ResultSet through the RowCallbackHandler and return nothing. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rch RowCallbackHandler with the processing implementation
	 * @throws DataAccessException
	 */
	void process(ResultSet resultSet, RowCallbackHandler rch);

	/**
	 * Executes the provided CQL Query, and maps all Rows returned with the supplied RowMapper.
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for mapping all rows
	 * @param optionsOrNull Query Options Object if exists
	 * @return List of <T> processed by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> query(String cql, RowMapper<T> rowMapper, QueryOptions optionsOrNull);

	/**
	 * Processes the ResultSet through the RowMapper and returns the List of mapped Rows. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rowMapper RowMapper with the processing implementation
	 * @return List of <T> generated by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper);

	/**
	 * Executes the provided CQL Query, and maps <b>ONE</b> Row returned with the supplied RowMapper.
	 * 
	 * <p>
	 * This expects only ONE row to be returned. More than one Row will cause an Exception to be thrown.
	 * </p>
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for convert the Row to <T>
	 * @param optionsOrNull Query Options Object if exists
	 * @return Object<T>
	 * @throws DataAccessException
	 */
	<T> T queryForObject(String cql, RowMapper<T> rowMapper, QueryOptions optionsOrNull);

	/**
	 * Process a ResultSet through a RowMapper. This is used internal to the Template for core operations, but is made
	 * available through Operations in the event you have a ResultSet to process. The ResultsSet could come from a
	 * ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param rowMapper
	 * @return
	 * @throws DataAccessException
	 */
	<T> T processOne(ResultSet resultSet, RowMapper<T> rowMapper);

	/**
	 * Executes the provided query and tries to return the first column of the first Row as a Class<T>.
	 * 
	 * @param cql The Query
	 * @param elementType Valid Class that Cassandra Data Types can be converted to.
	 * @param optionsOrNull Query Options Object if exists
	 * @return The Object<T> - item [0,0] in the result table of the query.
	 * @throws DataAccessException
	 */
	<T> T queryForObject(String cql, Class<T> elementType, QueryOptions optionsOrNull);

	/**
	 * Process a ResultSet, trying to convert the first columns of the first Row to Class<T>. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param elementType
	 * @return
	 * @throws DataAccessException
	 */
	<T> T processOne(ResultSet resultSet, Class<T> elementType);

	/**
	 * Executes the provided CQL Query and maps <b>ONE</b> Row to a basic Map of Strings and Objects. If more than one Row
	 * is returned from the Query, an exception will be thrown.
	 * 
	 * @param cql The Query
	 * @param optionsOrNull Query Options Object if exists
	 * @return Map representing the results of the Query
	 * @throws DataAccessException
	 */
	Map<String, Object> queryForMap(String cql, QueryOptions optionsOrNull);

	/**
	 * Process a ResultSet with <b>ONE</b> Row and convert to a Map. This is used internal to the Template for core
	 * operations, but is made available through Operations in the event you have a ResultSet to process. The ResultsSet
	 * could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @return
	 * @throws DataAccessException
	 */
	Map<String, Object> processMap(ResultSet resultSet);

	/**
	 * Executes the provided CQL and returns all values in the first column of the Results as a List of the Type in the
	 * second argument.
	 * 
	 * @param cql The Query
	 * @param elementType Type to cast the data values to
	 * @param optionsOrNull Query Options Object if exists
	 * @return List of elementType
	 * @throws DataAccessException
	 */
	<T> List<T> queryForList(String cql, Class<T> elementType, QueryOptions optionsOrNull);

	/**
	 * Process a ResultSet and convert the first column of the results to a List. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param elementType
	 * @return
	 * @throws DataAccessException
	 */
	<T> List<T> processList(ResultSet resultSet, Class<T> elementType);

	/**
	 * Executes the provided CQL and converts the results to a basic List of Maps. Each element in the List represents a
	 * Row returned from the Query. Each Row's columns are put into the map as column/value.
	 * 
	 * @param cql The Query
	 * @param optionsOrNull Query Options Object if exists
	 * @return List of Maps with the query results
	 * @throws DataAccessException
	 */
	List<Map<String, Object>> queryForListOfMap(String cql, QueryOptions optionsOrNull);

	/**
	 * Process a ResultSet and convert it to a List of Maps with column/value. This is used internal to the Template for
	 * core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @return
	 * @throws DataAccessException
	 */
	List<Map<String, Object>> processListOfMap(ResultSet resultSet);

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * PreparedStatementCallback implementation provided by the Application Code.
	 * 
	 * @param cql The CQL Statement to Execute
	 * @param action What to do with the results of the PreparedStatement
	 * @param optionsOrNull Query Options Object if exists
	 * @return Type<T> as determined by the supplied Callback.
	 * @throws DataAccessException
	 */
	<T> T execute(String cql, PreparedStatementCallback<T> action, QueryOptions optionsOrNull);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call, then executes the statement and processes
	 * the statement using the provided Callback. <b>This can only be used for CQL Statements that do not have data
	 * binding.</b> The results of the PreparedStatement are processed with PreparedStatementCallback implementation
	 * provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param action What to do with the results of the PreparedStatement
	 * @param optionsOrNull Query Options Object if exists
	 * @return Type<T> as determined by the supplied Callback.
	 * @throws DataAccessException
	 */
	<T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action, QueryOptions optionsOrNull);

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the ResultSetExtractor implementation provided by the Application Code. The can return any object,
	 * including a List of Objects to support the ResultSet processing.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rse The implementation for extracting the results of the query.
	 * @param optionsOrNull Query Options Object if exists
	 * @return Type<T> generated by the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse, QueryOptions optionsOrNull);

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowCallbackHandler implementation provided and nothing is returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rch The RowCallbackHandler for processing the ResultSet
	 * @param optionsOrNull Query Options Object if exists
	 * @throws DataAccessException
	 */
	void query(String cql, PreparedStatementBinder psb, RowCallbackHandler rch, QueryOptions optionsOrNull);

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowMapper implementation provided and a List is returned with elements of Type <T> for each Row
	 * returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rowMapper The implementation for Mapping a Row to Type <T>
	 * @param optionsOrNull Query Options Object if exists
	 * @return List of <T> for each Row returned from the Query.
	 * @throws DataAccessException
	 */
	<T> List<T> query(String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper, QueryOptions optionsOrNull);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rse Implementation for extracting from the ResultSet
	 * @param optionsOrNull Query Options Object if exists
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, QueryOptions optionsOrNull);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rch The implementation to process Results
	 * @param optionsOrNull Query Options Object if exists
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, RowCallbackHandler rch, QueryOptions optionsOrNull);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with RowMapper
	 * implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param optionsOrNull Query Options Object if exists
	 * @return List of Type <T> mapped from each Row in the Results
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper, QueryOptions optionsOrNull);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psbOrNull The implementation to bind variables to values if exists
	 * @param rse Implementation for extracting from the ResultSet
	 * @param optionsOrNull Query Options Object if exists
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, PreparedStatementBinder psbOrNull, ResultSetExtractor<T> rse,
			QueryOptions optionsOrNull);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psbOrNull The implementation to bind variables to values if exists
	 * @param rch The implementation to process Results
	 * @param optionsOrNull The Query Options Object if exists
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, PreparedStatementBinder psbOrNull, RowCallbackHandler rch,
			QueryOptions optionsOrNull);

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowMapper implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psbOrNull The implementation to bind variables to values if exists
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param optionsOrNull The Query Options Object if exists
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, PreparedStatementBinder psbOrNull, RowMapper<T> rowMapper,
			QueryOptions optionsOrNull);

	/**
	 * Describe the current Ring. This uses the provided {@link RingMemberHostMapper} to provide the basics of the
	 * Cassandra Ring topology.
	 * 
	 * @return The collection of ring tokens that are active in the cluster
	 */
	Collection<RingMember> describeRing();

	/**
	 * Describe the current Ring. Application code must provide its own {@link HostMapper} implementation to process the
	 * lists of hosts returned by the Cassandra Cluster Metadata.
	 * 
	 * @param hostMapper The implementation to use for host mapping.
	 * @return Collection generated by the provided HostMapper.
	 * @throws DataAccessException
	 */
	<T> Collection<T> describeRing(HostMapper<T> hostMapper);

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
	 * @param cql The CQL
	 * @param rowIterator Implementation to provide the Object[] to be bound to the CQL.
	 * @param optionsOrNull The Query Options Object if exists
	 */
	void ingest(String cql, Iterable<Object[]> rowIterator, QueryOptions optionsOrNull);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The Object[] length of the nested array must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows Object array of Object array of values to bind to the CQL.
	 * @param optionsOrNull The Query Options Object is exists
	 */
	void ingest(String cql, Object[][] rows, QueryOptions optionsOrNull);

	/**
	 * Delete all rows in the table
	 * 
	 * @param asynchronously
	 * @param tableName
	 * @param optionsOrNull
	 */
	void truncate(boolean asynchronously, String tableName, QueryOptions optionsOrNull);

}
