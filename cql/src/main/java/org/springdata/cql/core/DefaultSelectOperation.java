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
package org.springdata.cql.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Default implementation for SelectOperation
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultSelectOperation extends AbstractQueryOperation<ResultSet, SelectOperation> implements
		SelectOperation {

	private final Statement query;

	protected DefaultSelectOperation(CqlTemplate cqlTemplate, Statement query) {
		super(cqlTemplate);
		this.query = query;
	}

	@Override
	public SelectOneOperation firstRow() {
		return new DefaultSelectOneOperation(this, false);
	}

	@Override
	public SelectOneOperation singleResult() {
		return new DefaultSelectOneOperation(this, true);
	}

	@Override
	public <R> ProcessOperation<List<R>> map(final RowMapper<R> rowMapper) {

		return new ProcessingSelectOperation<List<R>>(this, new Processor<List<R>>() {

			@Override
			public List<R> process(ResultSet resultSet) {
				return cqlTemplate.process(resultSet, rowMapper);
			}

		});
	}

	@Override
	public ProcessOperation<Boolean> exists() {

		return new ProcessingSelectOperation<Boolean>(this, new Processor<Boolean>() {

			@Override
			public Boolean process(ResultSet resultSet) {
				return cqlTemplate.doProcess(resultSet, new ResultSetExtractor<Boolean>() {

					@Override
					public Boolean extractData(ResultSet resultSet) {
						return resultSet.iterator().hasNext();
					}

				});

			}

		});

	}

	@Override
	public <E> ProcessOperation<List<E>> firstColumn(final Class<E> elementType) {

		return new ProcessingSelectOperation<List<E>>(this, new Processor<List<E>>() {

			@Override
			public List<E> process(ResultSet resultSet) {
				return cqlTemplate.processFirstColumn(resultSet, elementType);
			}

		});
	}

	@Override
	public ProcessOperation<List<Map<String, Object>>> map() {

		return new ProcessingSelectOperation<List<Map<String, Object>>>(this, new Processor<List<Map<String, Object>>>() {

			@Override
			public List<Map<String, Object>> process(ResultSet resultSet) {
				return cqlTemplate.processAsMap(resultSet);
			}

		});

	}

	@Override
	public <O> ProcessOperation<O> transform(final ResultSetExtractor<O> rse) {

		return new ProcessingSelectOperation<O>(this, new Processor<O>() {

			@Override
			public O process(ResultSet resultSet) {
				return cqlTemplate.doProcess(resultSet, rse);
			}

		});
	}

	@Override
	public ProcessOperation<Object> forEach(final RowCallbackHandler rch) {

		return new ProcessingSelectOperation<Object>(this, new Processor<Object>() {

			@Override
			public Object process(ResultSet resultSet) {
				cqlTemplate.process(resultSet, rch);
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

	@Override
	public Statement toQuery() {
		return query;
	}

	abstract class ForwardingSelectOperation<T> implements ProcessOperation<T> {

		protected final SelectOperation delegate;

		private ForwardingSelectOperation(SelectOperation delegate) {
			this.delegate = delegate;
		}

		@Override
		public ProcessOperation<T> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
			delegate.withConsistencyLevel(consistencyLevel);
			return this;
		}

		@Override
		public ProcessOperation<T> withRetryPolicy(RetryPolicy retryPolicy) {
			delegate.withRetryPolicy(retryPolicy);
			return this;
		}

		@Override
		public ProcessOperation<T> withQueryTracing(Boolean queryTracing) {
			delegate.withQueryTracing(queryTracing);
			return this;
		}

		@Override
		public ProcessOperation<T> withFallbackHandler(FallbackHandler fh) {
			delegate.withFallbackHandler(fh);
			return this;
		}

		@Override
		public ProcessOperation<T> withExecutor(Executor executor) {
			delegate.withExecutor(executor);
			return this;
		}

		@Override
		public Statement toQuery() {
			return delegate.toQuery();
		}

	}

	interface Processor<T> {
		T process(ResultSet resultSet);
	}

	final class ProcessingSelectOperation<T> extends ForwardingSelectOperation<T> {

		private final Processor<T> processor;

		ProcessingSelectOperation(SelectOperation delegate, Processor<T> processor) {
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

			}, getExecutor());

			return new CassandraFuture<T>(future, cqlTemplate.getExceptionTranslator());
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
				fireOnFailure(e);
				throw e;
			}
		}

	}

	final class DefaultSelectOneOperation extends AbstractSelectOneOperation {

		private final DefaultSelectOperation defaultSelectOperation;
		private final boolean singleResult;

		DefaultSelectOneOperation(DefaultSelectOperation defaultSelectOperation, boolean singleResult) {
			super(defaultSelectOperation.cqlTemplate, defaultSelectOperation.query, singleResult);
			this.defaultSelectOperation = defaultSelectOperation;
			this.singleResult = singleResult;
		}

		@Override
		public <R> ProcessOperation<R> map(final RowMapper<R> rowMapper) {

			return new ProcessingSelectOperation<R>(defaultSelectOperation, new Processor<R>() {

				@Override
				public R process(ResultSet resultSet) {
					return cqlTemplate.processOne(resultSet, rowMapper, singleResult);
				}

			});

		}

		@Override
		public <E> ProcessOperation<E> firstColumn(final Class<E> elementType) {

			return new ProcessingSelectOperation<E>(defaultSelectOperation, new Processor<E>() {

				@Override
				public E process(ResultSet resultSet) {
					return cqlTemplate.processOneFirstColumn(resultSet, elementType, singleResult);
				}

			});
		}

		@Override
		public ProcessOperation<Map<String, Object>> map() {

			return new ProcessingSelectOperation<Map<String, Object>>(defaultSelectOperation,
					new Processor<Map<String, Object>>() {

						@Override
						public Map<String, Object> process(ResultSet resultSet) {
							return cqlTemplate.processOneAsMap(resultSet, singleResult);
						}

					});
		}

	}
}
