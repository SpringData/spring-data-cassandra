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
package org.springdata.cassandra.base.core;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import org.springdata.cassandra.base.core.query.ConsistencyLevel;
import org.springdata.cassandra.base.core.query.RetryPolicy;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultSelectOperation extends AbstractQueryOperation<ResultSet, SelectOperation<ResultSet>> implements
		SelectOperation<ResultSet> {

	private final Query query;

	protected DefaultSelectOperation(CassandraTemplate cassandraTemplate, Query query) {
		super(cassandraTemplate);
		this.query = query;
	}

	@Override
	public <R> BaseSelectOperation<Iterator<R>> map(final RowMapper<R> rowMapper) {

		return new ProcessingSelectOperation<Iterator<R>>(this, new Processor<Iterator<R>>() {

			@Override
			public Iterator<R> process(ResultSet resultSet) {
				return cassandraTemplate.process(resultSet, rowMapper);
			}

		});
	}

	@Override
	public <R> BaseSelectOperation<R> mapOne(final RowMapper<R> rowMapper) {

		return new ProcessingSelectOperation<R>(this, new Processor<R>() {

			@Override
			public R process(ResultSet resultSet) {
				return cassandraTemplate.processOne(resultSet, rowMapper);
			}

		});
	}

	@Override
	public <E> BaseSelectOperation<E> firstColumnOne(final Class<E> elementType) {

		return new ProcessingSelectOperation<E>(this, new Processor<E>() {

			@Override
			public E process(ResultSet resultSet) {
				return cassandraTemplate.processOneFirstColumn(resultSet, elementType);
			}

		});
	}

	@Override
	public <E> BaseSelectOperation<Iterator<E>> firstColumn(final Class<E> elementType) {

		return new ProcessingSelectOperation<Iterator<E>>(this, new Processor<Iterator<E>>() {

			@Override
			public Iterator<E> process(ResultSet resultSet) {
				return cassandraTemplate.processFirstColumnAsList(resultSet, elementType).iterator();
			}

		});
	}

	@Override
	public BaseSelectOperation<Iterator<Map<String, Object>>> map() {

		return new ProcessingSelectOperation<Iterator<Map<String, Object>>>(this,
				new Processor<Iterator<Map<String, Object>>>() {

					@Override
					public Iterator<Map<String, Object>> process(ResultSet resultSet) {
						return cassandraTemplate.processAsListOfMap(resultSet).iterator();
					}

				});

	}

	@Override
	public BaseSelectOperation<Map<String, Object>> mapOne() {

		return new ProcessingSelectOperation<Map<String, Object>>(this, new Processor<Map<String, Object>>() {

			@Override
			public Map<String, Object> process(ResultSet resultSet) {
				return cassandraTemplate.processOneAsMap(resultSet);
			}

		});
	}

	@Override
	public <O> BaseSelectOperation<O> transform(final ResultSetCallback<O> rsc) {

		return new ProcessingSelectOperation<O>(this, new Processor<O>() {

			@Override
			public O process(ResultSet resultSet) {
				return cassandraTemplate.doProcess(resultSet, rsc);
			}

		});
	}

	@Override
	public BaseSelectOperation<Object> each(final RowCallbackHandler rch) {

		return new ProcessingSelectOperation<Object>(this, new Processor<Object>() {

			@Override
			public Object process(ResultSet resultSet) {
				cassandraTemplate.process(resultSet, rch);
				return null;
			}

		});
	}

	@Override
	public ResultSet execute() {
		return doExecute(query);
	}

	@Override
	public CassandraFuture<ResultSet> executeAsync() {
		return doExecuteAsync(query);
	}

	@Override
	public void executeAsync(CallbackHandler<ResultSet> cb) {
		doExecuteAsync(query, cb);
	}

	@Override
	public ResultSet executeNonstop(int timeoutMls) throws TimeoutException {
		return doExecuteNonstop(query, timeoutMls);
	}

	abstract class ForwardingSelectOperation<T> implements BaseSelectOperation<T> {

		protected final SelectOperation<ResultSet> delegate;

		private ForwardingSelectOperation(SelectOperation<ResultSet> delegate) {
			this.delegate = delegate;
		}

		@Override
		public BaseSelectOperation<T> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
			delegate.withConsistencyLevel(consistencyLevel);
			return this;
		}

		@Override
		public BaseSelectOperation<T> withRetryPolicy(RetryPolicy retryPolicy) {
			delegate.withRetryPolicy(retryPolicy);
			return this;
		}

		@Override
		public BaseSelectOperation<T> withQueryTracing(Boolean queryTracing) {
			delegate.withQueryTracing(queryTracing);
			return this;
		}

		@Override
		public BaseSelectOperation<T> withFallbackHandler(FallbackHandler fh) {
			delegate.withFallbackHandler(fh);
			return this;
		}

		@Override
		public BaseSelectOperation<T> withExecutor(Executor executor) {
			delegate.withExecutor(executor);
			return this;
		}

	}

	interface Processor<T> {
		T process(ResultSet resultSet);
	}

	final class ProcessingSelectOperation<T> extends ForwardingSelectOperation<T> {

		private final Processor<T> processor;

		ProcessingSelectOperation(SelectOperation<ResultSet> delegate, Processor<T> processor) {
			super(delegate);
			this.processor = processor;
		}

		@Override
		public T execute() {
			ResultSet resultSet = delegate.execute();
			return processor.process(resultSet);
		}

		@Override
		public CassandraFuture<T> executeAsync() {

			CassandraFuture<ResultSet> resultSetFuture = delegate.executeAsync();

			ListenableFuture<T> future = Futures.transform(resultSetFuture, new Function<ResultSet, T>() {

				@Override
				public T apply(ResultSet resultSet) {
					return processWithFallback(resultSet);
				}

			}, executor != null ? executor : MoreExecutors.sameThreadExecutor());

			return new CassandraFuture<T>(future, cassandraTemplate.getExceptionTranslator());
		}

		@Override
		public void executeAsync(final CallbackHandler<T> cb) {
			delegate.executeAsync(new CallbackHandler<ResultSet>() {

				@Override
				public void onComplete(ResultSet resultSet) {
					T result = processWithFallback(resultSet);
					cb.onComplete(result);
				}

			});
		}

		@Override
		public T executeNonstop(int timeoutMls) throws TimeoutException {
			ResultSet resultSet = delegate.executeNonstop(timeoutMls);
			return processor.process(resultSet);

		}

		protected T processWithFallback(ResultSet resultSet) {
			try {
				return processor.process(resultSet);
			} catch (RuntimeException e) {
				if (fh != null) {
					fh.onFailure(e);
				}
				throw e;
			}
		}

	}

}
