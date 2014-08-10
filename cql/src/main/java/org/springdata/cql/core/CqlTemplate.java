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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cql.support.CassandraExceptionTranslator;
import org.springdata.cql.support.exception.CassandraNotSingleResultException;
import org.springdata.cql.support.exception.CassandraQueryAware;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * <b>This is the Central class in the Cassandra core package.</b> It simplifies the use of Cassandra and helps to avoid
 * common errors. It executes the core Cassandra workflow, leaving application code to provide CQL and result
 * extraction. This class execute CQL Queries, provides different ways to extract/map results, and provides Exception
 * translation to the generic, more informative exception hierarchy defined in the <code>org.springframework.dao</code>
 * package.
 * 
 * <p>
 * For working with POJOs, use the CassandraTemplate.
 * </p>
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 */
public class CqlTemplate implements CqlOperations {

	private final static Logger logger = LoggerFactory.getLogger(CqlTemplate.class);

	private Session session;
	private String keyspace;

	private CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	private AdminCqlOperations adminOperations;
	private SchemaCqlOperations schemaOperations;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 * @param keyspace must not be {@literal null}.
	 */
	public CqlTemplate(Session session, String keyspace) {
		setSession(session);
		setKeyspace(keyspace);
		this.adminOperations = new DefaultAdminCqlOperations(this);
		this.schemaOperations = new DefaultSchemaCqlOperations(this, keyspace);
	}

	/**
	 * Ensure that the Cassandra Session has been set
	 */
	public void afterPropertiesSet() {
		if (getSession() == null) {
			throw new IllegalArgumentException("Property 'session' is required");
		}
		if (getKeyspace() == null) {
			throw new IllegalArgumentException("Property 'keyspace' is required");
		}
	}

	/**
	 * @return Returns the session.
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * @param session The session to set.
	 */
	public void setSession(Session session) {
		Assert.notNull(session);
		this.session = session;
	}

	/**
	 * @return Returns the keyspace.
	 */
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * @param keyspace The keyspace to set.
	 */
	public void setKeyspace(String keyspace) {
		Assert.notNull(session);
		this.keyspace = keyspace;
	}

	/**
	 * Set the exception translator for this instance.
	 * 
	 * @see org.springdata.cql.support.CassandraExceptionTranslator
	 */
	public void setExceptionTranslator(CassandraExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Return the exception translator for this instance.
	 */
	public CassandraExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	@Override
	public Statement createStatement(StatementCreator qc) {
		Assert.notNull(qc);
		return doCreateQuery(qc);
	}

	@Override
	public <T> T execute(SessionCallback<T> sessionCallback) {
		Assert.notNull(sessionCallback);
		return doExecute(sessionCallback);
	}

	@Override
	public ResultSet execute(Statement query) {
		Assert.notNull(query);
		return doExecute(query);
	}

	@Override
	public ResultSet execute(String cql) {
		Assert.notNull(cql);
		return doExecute(new SimpleStatement(cql));
	}

	@Override
	public ExecuteOperation buildExecuteOperation(String cql) {
		Assert.notNull(cql);
		return new DefaultExecuteOperation(this, cql);
	}

	@Override
	public ExecuteOperation buildExecuteOperation(Statement query) {
		Assert.notNull(query);
		return new DefaultExecuteOperation(this, query);
	}

	@Override
	public ResultSet execute(PreparedStatement ps, PreparedStatementBinder psb) {
		return buildExecuteOperation(ps, psb).execute();
	}

	@Override
	public ExecuteOperation buildExecuteOperation(PreparedStatement ps, PreparedStatementBinder psb) {
		Assert.notNull(ps);
		return new DefaultExecuteOperation(this, new SimplePreparedStatementQueryCreator(ps, psb));
	}

	@Override
	public ResultSet execute(BoundStatement bs) {
		return buildExecuteOperation(bs).execute();
	}

	@Override
	public ExecuteOperation buildExecuteOperation(final BoundStatement bs) {
		Assert.notNull(bs);
		return new DefaultExecuteOperation(this, new StatementCreator() {

			@Override
			public Statement createStatement() {
				return bs;
			}

		});
	}

	@Override
	public ResultSet execute(StatementCreator qc) {
		return buildExecuteOperation(qc).execute();
	}

	@Override
	public ExecuteOperation buildExecuteOperation(StatementCreator qc) {
		Assert.notNull(qc);
		return new DefaultExecuteOperation(this, qc);
	}

	@Override
	public ResultSet executeInBatch(String[] cqls) {
		return buildExecuteInBatchOperation(cqls).execute();
	}

	@Override
	public ExecuteOperation buildExecuteInBatchOperation(final String[] cqls) {
		Assert.notNull(cqls);

		final Iterator<RegularStatement> statements = Iterators.transform(new ArrayIterator<String>(cqls),
				new Function<String, RegularStatement>() {

					@Override
					public RegularStatement apply(String cql) {
						return new SimpleStatement(cql);
					}

				});

		return buildExecuteInBatchOperation(new Iterable<RegularStatement>() {

			@Override
			public Iterator<RegularStatement> iterator() {
				return statements;
			}

		});
	}

	@Override
	public ResultSet executeInBatch(Iterable<RegularStatement> statements) {
		return buildExecuteInBatchOperation(statements).execute();
	}

	@Override
	public ExecuteOperation buildExecuteInBatchOperation(final Iterable<RegularStatement> statements) {
		Assert.notNull(statements);

		return new DefaultExecuteOperation(this, new StatementCreator() {

			@Override
			public Statement createStatement() {

				/*
				 * Return variable is a Batch statement
				 */
				final Batch batch = QueryBuilder.batch();

				boolean emptyBatch = true;
				for (RegularStatement statement : statements) {

					Assert.notNull(statement);

					batch.add(statement);
					emptyBatch = false;
				}

				if (emptyBatch) {
					throw new IllegalArgumentException("statements are empty");
				}

				return batch;
			}

		});
	}

	@Override
	public SelectOperation buildSelectOperation(String cql) {
		Assert.notNull(cql);
		Statement query = new SimpleStatement(cql);
		return new DefaultSelectOperation(this, query);
	}

	@Override
	public SelectOperation buildSelectOperation(PreparedStatement ps, PreparedStatementBinder psb) {
		Assert.notNull(ps);
		BoundStatement bs = doBind(ps, psb);
		return new DefaultSelectOperation(this, bs);
	}

	@Override
	public SelectOperation buildSelectOperation(BoundStatement bs) {
		Assert.notNull(bs);
		return new DefaultSelectOperation(this, bs);
	}

	@Override
	public SelectOperation buildSelectOperation(StatementCreator qc) {
		Assert.notNull(qc);
		Statement query = doCreateQuery(qc);
		return new DefaultSelectOperation(this, query);
	}

	/**
	 * Execute a query creator
	 * 
	 * @param callback
	 * @return
	 */
	protected Statement doCreateQuery(StatementCreator qc) {

		try {

			return qc.createStatement();

		} catch (RuntimeException e) {
			throw translateIfPossible(e);
		}
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doExecute(SessionCallback<T> callback) {

		try {

			return callback.doInSession(getSession());

		} catch (RuntimeException e) {
			throw translateIfPossible(e);
		}
	}

	/**
	 * Execute as a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected ResultSet doExecute(final Statement query) {

		if (logger.isDebugEnabled()) {
			logger.debug(query.toString());
		}

		try {

			return getSession().execute(query);

		} catch (RuntimeException e) {
			e = translateIfPossible(e);
			if (e instanceof CassandraQueryAware) {
				((CassandraQueryAware) e).setQuery(query);
			}
			throw e;
		}

	}

	/**
	 * Execute as a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected ResultSetFuture doExecuteAsync(final Statement query) {

		if (logger.isDebugEnabled()) {
			logger.debug(query.toString());
		}

		try {

			return getSession().executeAsync(query);

		} catch (RuntimeException e) {
			e = translateIfPossible(e);
			if (e instanceof CassandraQueryAware) {
				((CassandraQueryAware) e).setQuery(query);
			}
			throw e;
		}

	}

	/**
	 * Deserializes first column in the row.
	 * 
	 * @param row
	 * @return
	 */
	protected Object firstColumnToObject(Row row) {
		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		if (cols.size() == 0) {
			return null;
		}
		return cols.getType(0).deserialize(row.getBytesUnsafe(0));
	}

	/**
	 * Deserializes row to the map.
	 * 
	 * @param row
	 * @return
	 */
	protected Map<String, Object> toMap(Row row) {
		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		Map<String, Object> map = new HashMap<String, Object>(cols.size());

		for (Definition def : cols.asList()) {
			String name = def.getName();
			DataType dataType = def.getType();
			map.put(name, dataType.deserialize(row.getBytesUnsafe(name)));
		}

		return map;
	}

	@Override
	public List<RingMember> describeRing() {
		return Collections.unmodifiableList(describeRing(new RingMemberHostMapper()));
	}

	/**
	 * Pulls the list of Hosts for the current Session
	 * 
	 * @return
	 */
	protected Set<Host> getHosts() {

		/*
		 * Get the cluster metadata for this session
		 */
		Metadata clusterMetadata = doExecute(new SessionCallback<Metadata>() {

			@Override
			public Metadata doInSession(Session s) {
				return s.getCluster().getMetadata();
			}

		});

		/*
		 * Get all hosts in the cluster
		 */
		Set<Host> hosts = clusterMetadata.getAllHosts();

		return hosts;

	}

	/**
	 * Process result and handle exceptions
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doProcess(final ResultSet resultSet, final ResultSetExtractor<T> callback) {

		try {

			return callback.extractData(resultSet);

		} catch (RuntimeException e) {
			throw translateIfPossible(e);
		}
	}

	@Override
	public <T> List<T> describeRing(HostMapper<T> hostMapper) {
		Assert.notNull(hostMapper);

		Set<Host> hosts = getHosts();

		List<T> results = new ArrayList<T>(hosts.size());
		for (Host host : hosts) {
			T obj = hostMapper.mapHost(host);
			results.add(obj);
		}

		return results;
	}

	@Override
	public void process(ResultSet resultSet, final RowCallbackHandler rch) {
		Assert.notNull(resultSet);
		Assert.notNull(rch);

		doProcess(resultSet, new ResultSetExtractor<Object>() {

			@Override
			public Object extractData(ResultSet resultSet) {

				for (Row row : resultSet) {
					rch.processRow(row);
				}

				return null;
			}

		});
	}

	@Override
	public <T> List<T> process(ResultSet resultSet, final RowMapper<T> rowMapper) {
		Assert.notNull(resultSet);
		Assert.notNull(rowMapper);

		return doProcess(resultSet, new ResultSetExtractor<List<T>>() {

			@Override
			public List<T> extractData(ResultSet resultSet) {

				List<T> result = Lists.newArrayList();

				int rowNum = 0;
				for (Row row : resultSet) {
					T obj = rowMapper.mapRow(row, rowNum++);
					result.add(obj);
				}

				return Collections.unmodifiableList(result);

			}

		});

	}

	@Override
	public <T> T processOne(ResultSet resultSet, final RowMapper<T> rowMapper, final boolean singleResult) {
		Assert.notNull(resultSet);
		Assert.notNull(rowMapper);

		return doProcess(resultSet, new ResultSetExtractor<T>() {

			@Override
			public T extractData(ResultSet resultSet) {

				Iterator<Row> iterator = resultSet.iterator();
				if (!iterator.hasNext()) {
					return null;
				}

				Row firstRow = iterator.next();

				if (singleResult && iterator.hasNext()) {
					throw new CassandraNotSingleResultException(resultSet);
				}

				return rowMapper.mapRow(firstRow, 0);
			}

		});

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T processOneFirstColumn(ResultSet resultSet, Class<T> elementType, final boolean singleResult) {
		Assert.notNull(resultSet);
		Assert.notNull(elementType);

		return doProcess(resultSet, new ResultSetExtractor<T>() {

			@Override
			public T extractData(ResultSet resultSet) {

				Iterator<Row> iterator = resultSet.iterator();
				if (!iterator.hasNext()) {
					return null;
				}

				Row firstRow = iterator.next();

				if (singleResult && iterator.hasNext()) {
					throw new CassandraNotSingleResultException(resultSet);
				}

				return (T) firstColumnToObject(firstRow);

			}

		});

	}

	@Override
	public Map<String, Object> processOneAsMap(ResultSet resultSet, final boolean singleResult) {
		Assert.notNull(resultSet);

		return doProcess(resultSet, new ResultSetExtractor<Map<String, Object>>() {

			@Override
			public Map<String, Object> extractData(ResultSet resultSet) {

				Iterator<Row> iterator = resultSet.iterator();
				if (!iterator.hasNext()) {
					return Collections.emptyMap();
				}

				Row firstRow = iterator.next();

				if (singleResult && iterator.hasNext()) {
					throw new CassandraNotSingleResultException(resultSet);
				}

				return toMap(firstRow);

			}

		});

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> processFirstColumn(ResultSet resultSet, Class<T> elementType) {
		Assert.notNull(resultSet);
		Assert.notNull(elementType);

		return doProcess(resultSet, new ResultSetExtractor<List<T>>() {

			@Override
			public List<T> extractData(ResultSet resultSet) {

				List<T> result = new ArrayList<T>();
				for (Row row : resultSet) {
					T obj = (T) firstColumnToObject(row);
					result.add(obj);
				}

				return Collections.unmodifiableList(result);

			}

		});

	}

	@Override
	public List<Map<String, Object>> processAsMap(ResultSet resultSet) {
		Assert.notNull(resultSet);

		return doProcess(resultSet, new ResultSetExtractor<List<Map<String, Object>>>() {

			@Override
			public List<Map<String, Object>> extractData(ResultSet resultSet) {

				List<Map<String, Object>> result = Lists.newArrayList();

				for (Row row : resultSet) {
					Map<String, Object> map = toMap(row);
					result.add(map);
				}

				return Collections.unmodifiableList(result);

			}

		});

	}

	@Override
	public PreparedStatement prepareStatement(String cql) {
		Assert.notNull(cql);
		return doPrepareStatement(new SimplePreparedStatementCreator(cql));
	}

	@Override
	public PreparedStatement prepareStatement(PreparedStatementCreator psc) {
		Assert.notNull(psc);
		return doPrepareStatement(psc);
	}

	/**
	 * Service method to prepare statement
	 * 
	 * @param cql
	 * @param consistency
	 * @param retryPolicy
	 * @param traceQuery
	 * @return
	 */

	protected PreparedStatement doPrepareStatement(final PreparedStatementCreator psc) {

		return doExecute(new SessionCallback<PreparedStatement>() {

			@Override
			public PreparedStatement doInSession(Session session) {

				PreparedStatement ps = psc.createPreparedStatement(session);

				return ps;
			}

		});

	}

	/**
	 * Service method to deal with PreparedStatements
	 * 
	 * @param psc
	 * @param rsc
	 * @param optionsOrNull
	 * @return
	 */

	protected <T> T doExecute(final PreparedStatement ps, final PreparedStatementCallback<T> rsc) {

		return doExecute(new SessionCallback<T>() {

			@Override
			public T doInSession(Session session) {
				return rsc.doWithPreparedStatement(session, ps);
			}

		});
	}

	@Override
	public <T> T execute(final PreparedStatement ps, final PreparedStatementCallback<T> rsc) {

		Assert.notNull(ps);
		Assert.notNull(rsc);

		return doExecute(ps, rsc);

	}

	/**
	 * Service method to bind PreparedStatement
	 * 
	 * @param ps
	 * @param psbOrNull
	 * @return
	 */

	protected BoundStatement doBind(final PreparedStatement ps, final PreparedStatementBinder psbOrNull) {

		Assert.notNull(ps);

		return doExecute(new SessionCallback<BoundStatement>() {

			@Override
			public BoundStatement doInSession(Session session) {

				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}

				return bs;
			}

		});

	}

	@Override
	public BoundStatement bind(PreparedStatement ps) {
		return doBind(ps, null);
	}

	@Override
	public BoundStatement bind(PreparedStatement ps, PreparedStatementBinder psb) {
		return doBind(ps, psb);
	}

	@Override
	public List<ResultSet> ingest(PreparedStatement ps, Iterable<Object[]> rows) {
		return buildIngestOperation(ps, rows).execute();
	}

	@Override
	public IngestOperation buildIngestOperation(final PreparedStatement ps, Iterable<Object[]> rows) {

		Assert.notNull(ps);
		Assert.notNull(rows);

		Iterator<Statement> queryIterator = Iterators.transform(rows.iterator(), new Function<Object[], Statement>() {

			@Override
			public Statement apply(final Object[] values) {

				BoundStatement bs = doBind(ps, new PreparedStatementBinder() {

					@Override
					public BoundStatement bindValues(PreparedStatement ps) {
						return ps.bind(values);
					}
				});

				return bs;
			}
		});

		return new DefaultIngestOperation(this, queryIterator);

	}

	@Override
	public List<ResultSet> ingest(PreparedStatement ps, Object[][] rows) {
		return buildIngestOperation(ps, rows).execute();
	}

	@Override
	public IngestOperation buildIngestOperation(PreparedStatement ps, final Object[][] rows) {

		Assert.notNull(ps);
		Assert.notNull(rows);
		Assert.notEmpty(rows);

		final Iterator<Object[]> iterator = new ArrayIterator<Object[]>(rows);

		return buildIngestOperation(ps, new Iterable<Object[]>() {

			@Override
			public Iterator<Object[]> iterator() {
				return iterator;
			}

		});
	}

	/**
	 * Service iterator based on array
	 * 
	 * @author Alex Shvid
	 * 
	 * @param <T>
	 */

	private static class ArrayIterator<T> implements Iterator<T> {

		private T[] array;
		private int pos = 0;

		public ArrayIterator(T[] array) {
			this.array = array;
		}

		public boolean hasNext() {
			return array.length > pos;
		}

		public T next() {
			return array[pos++];
		}

		public void remove() {
			throw new UnsupportedOperationException("Cannot remove an element of an array.");
		}

	}

	@Override
	public Long countAll(String tableName) {
		return buildCountAllOperation(tableName).execute();
	}

	@Override
	public ProcessOperation<Long> buildCountAllOperation(final String tableName) {
		Assert.notNull(tableName);

		return buildSelectOperation(new StatementCreator() {

			@Override
			public Statement createStatement() {
				Select select = QueryBuilder.select().countAll().from(tableName);
				return select;
			}

		}).singleResult().firstColumn(Long.class);
	}

	@Override
	public ResultSet truncate(String tableName) {
		return buildTruncateOperation(tableName).execute();
	}

	@Override
	public ExecuteOperation buildTruncateOperation(final String tableName) {
		Assert.notNull(tableName);
		return new DefaultExecuteOperation(this, new StatementCreator() {

			@Override
			public Statement createStatement() {
				return QueryBuilder.truncate(tableName);
			}

		});
	}

	@Override
	public AdminCqlOperations getAdminOperations() {
		return adminOperations;
	}

	@Override
	public SchemaCqlOperations getSchemaOperations() {
		return schemaOperations;
	}

	/**
	 * Attempt to translate a Runtime Exception to a Spring Data Exception
	 * 
	 * @param ex
	 * @return
	 */
	public RuntimeException translateIfPossible(RuntimeException ex) {
		RuntimeException resolved = getExceptionTranslator().translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

}
