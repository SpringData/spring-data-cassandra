/*
 * Copyright 2014 the original author or authors.
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

import java.util.concurrent.TimeoutException;

import org.springdata.cql.core.AbstractQueryOperation;
import org.springdata.cql.core.CallbackHandler;
import org.springdata.cql.core.CassandraFuture;
import org.springdata.cql.core.CqlTemplate;
import org.springdata.cql.core.QueryCreator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Abstract Get Operation
 * 
 * @author Alex Shvid
 * 
 * @param <T> - return Type
 */

public abstract class AbstractGetOperation<T> extends AbstractQueryOperation<T, GetOperation<T>> implements
		GetOperation<T>, QueryCreator {

	private String tableName;

	public abstract T transform(ResultSet resultSet);

	public AbstractGetOperation(CqlTemplate cqlTemplate) {
		super(cqlTemplate);
	}

	@Override
	public GetOperation<T> formTable(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public T execute() {
		Statement query = doCreateQuery(this);
		ResultSet resultSet = doExecute(query);
		return transform(resultSet);
	}

	@Override
	public CassandraFuture<T> executeAsync() {
		Statement query = doCreateQuery(this);
		CassandraFuture<ResultSet> resultSetFuture = doExecuteAsync(query);

		ListenableFuture<T> future = Futures.transform(resultSetFuture, new Function<ResultSet, T>() {

			@Override
			public T apply(ResultSet resultSet) {
				return processWithFallback(resultSet);
			}

		}, getExecutor());

		return new CassandraFuture<T>(future, cqlTemplate.getExceptionTranslator());

	}

	@Override
	public void executeAsync(final CallbackHandler<T> cb) {

		Statement query = doCreateQuery(this);
		doExecuteAsync(query, new CallbackHandler<ResultSet>() {

			@Override
			public void onComplete(ResultSet resultSet) {
				T result = processWithFallback(resultSet);
				cb.onComplete(result);
			}

		});
	}

	@Override
	public T executeNonstop(int timeoutMls) throws TimeoutException {
		Statement query = doCreateQuery(this);
		ResultSet resultSet = doExecuteNonstop(query, timeoutMls);
		return transform(resultSet);
	}

	@Override
	public Statement toStatement() {
		return doCreateQuery(this);
	}

	protected T processWithFallback(ResultSet resultSet) {
		try {
			return transform(resultSet);
		} catch (RuntimeException e) {
			fireOnFailure(e);
			throw e;
		}
	}

}
