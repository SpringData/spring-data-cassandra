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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.cql.core.AbstractQueryOperation;
import org.springdata.cassandra.cql.core.CallbackHandler;
import org.springdata.cassandra.cql.core.CassandraCqlTemplate;
import org.springdata.cassandra.cql.core.CassandraFuture;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Abstract Multi Get Operation
 * 
 * @author Alex Shvid
 * 
 * @param <T> - return Type
 */

public abstract class AbstractMultiGetOperation<T> extends AbstractQueryOperation<T, GetOperation<T>> implements
		GetOperation<T> {

	private String tableName;

	public abstract Iterator<Query> getQueryIterator();

	public abstract T transform(List<ResultSet> resultSets);

	public AbstractMultiGetOperation(CassandraCqlTemplate cassandraCqlTemplate) {
		super(cassandraCqlTemplate);
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
		Iterator<Query> queryIterator = getQueryIterator();
		List<ResultSet> resultSets = doExecute(queryIterator);
		return transform(resultSets);
	}

	@Override
	public CassandraFuture<T> executeAsync() {
		Iterator<Query> queryIterator = getQueryIterator();
		CassandraFuture<List<ResultSet>> resultSetsFuture = doExecuteAsync(queryIterator);

		ListenableFuture<T> future = Futures.transform(resultSetsFuture, new Function<List<ResultSet>, T>() {

			@Override
			public T apply(List<ResultSet> resultSets) {
				return processWithFallback(resultSets);
			}

		}, getExecutor());

		return new CassandraFuture<T>(future, cassandraCqlTemplate.getExceptionTranslator());

	}

	@Override
	public void executeAsync(final CallbackHandler<T> cb) {

		Iterator<Query> queryIterator = getQueryIterator();
		doExecuteAsync(queryIterator, new CallbackHandler<List<ResultSet>>() {

			@Override
			public void onComplete(List<ResultSet> resultSets) {
				T result = processWithFallback(resultSets);
				cb.onComplete(result);
			}

		});
	}

	@Override
	public T executeNonstop(int timeoutMls) throws TimeoutException {
		Iterator<Query> queryIterator = getQueryIterator();
		List<ResultSet> resultSets = doExecuteNonstop(queryIterator, timeoutMls);
		return transform(resultSets);
	}

	protected T processWithFallback(List<ResultSet> resultSets) {
		try {
			return transform(resultSets);
		} catch (RuntimeException e) {
			fireOnFailure(e);
			throw e;
		}
	}

}
