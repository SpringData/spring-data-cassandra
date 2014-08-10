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
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Default implementation for @QueryOperation
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultQueryOperation extends AbstractStatementOperation<ResultSet, QueryOperation> implements
		QueryOperation {

	private final Statement statement;

	protected DefaultQueryOperation(CqlTemplate cqlTemplate, Statement statement) {
		super(cqlTemplate);
		this.statement = statement;
	}

	@Override
	public SingleResultQueryOperation firstRow() {
		return new DefaultSingleResultQueryOperation(this, false);
	}

	@Override
	public SingleResultQueryOperation singleResult() {
		return new DefaultSingleResultQueryOperation(this, true);
	}

	@Override
	public <R> TransformOperation<List<R>> map(final RowMapper<R> rowMapper) {

		return new ProcessingQueryOperation<List<R>>(this, new Processor<List<R>>() {

			@Override
			public List<R> process(ResultSet resultSet) {
				return cqlTemplate.process(resultSet, rowMapper);
			}

		});
	}

	@Override
	public TransformOperation<Boolean> exists() {

		return new ProcessingQueryOperation<Boolean>(this, new Processor<Boolean>() {

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
	public <E> TransformOperation<List<E>> firstColumn(final Class<E> elementType) {

		return new ProcessingQueryOperation<List<E>>(this, new Processor<List<E>>() {

			@Override
			public List<E> process(ResultSet resultSet) {
				return cqlTemplate.processFirstColumn(resultSet, elementType);
			}

		});
	}

	@Override
	public TransformOperation<List<Map<String, Object>>> map() {

		return new ProcessingQueryOperation<List<Map<String, Object>>>(this, new Processor<List<Map<String, Object>>>() {

			@Override
			public List<Map<String, Object>> process(ResultSet resultSet) {
				return cqlTemplate.processAsMap(resultSet);
			}

		});

	}

	@Override
	public <O> TransformOperation<O> transform(final ResultSetExtractor<O> rse) {

		return new ProcessingQueryOperation<O>(this, new Processor<O>() {

			@Override
			public O process(ResultSet resultSet) {
				return cqlTemplate.doProcess(resultSet, rse);
			}

		});
	}

	@Override
	public TransformOperation<Object> forEach(final RowCallbackHandler rch) {

		return new ProcessingQueryOperation<Object>(this, new Processor<Object>() {

			@Override
			public Object process(ResultSet resultSet) {
				cqlTemplate.process(resultSet, rch);
				return null;
			}

		});
	}

	@Override
	public ResultSet execute() {
		return doExecute(statement);
	}

	@Override
	public CassandraFuture<ResultSet> executeAsync() {
		return doExecuteAsync(statement);
	}

	@Override
	public void executeAsync(CallbackHandler<ResultSet> cb) {
		doExecuteAsync(statement, cb);
	}

	@Override
	public ResultSet executeNonstop(int timeoutMls) throws TimeoutException {
		return doExecuteNonstop(statement, timeoutMls);
	}

	@Override
	public Statement toStatement() {
		return statement;
	}

	abstract class ForwardingQueryOperation<T> implements TransformOperation<T> {

		protected final QueryOperation delegate;

		private ForwardingQueryOperation(QueryOperation delegate) {
			this.delegate = delegate;
		}

		@Override
		public TransformOperation<T> withConsistencyLevel(ConsistencyLevel consistencyLevel) {
			delegate.withConsistencyLevel(consistencyLevel);
			return this;
		}

		@Override
		public TransformOperation<T> withRetryPolicy(RetryPolicy retryPolicy) {
			delegate.withRetryPolicy(retryPolicy);
			return this;
		}

		@Override
		public TransformOperation<T> withRetryPolicy(RetryPolicyInstance retryPolicy) {
			delegate.withRetryPolicy(retryPolicy);
			return this;
		}

		@Override
		public TransformOperation<T> withQueryTracing(Boolean queryTracing) {
			delegate.withQueryTracing(queryTracing);
			return this;
		}

		@Override
		public TransformOperation<T> withFallbackHandler(FallbackHandler fh) {
			delegate.withFallbackHandler(fh);
			return this;
		}

		@Override
		public TransformOperation<T> withExecutor(Executor executor) {
			delegate.withExecutor(executor);
			return this;
		}

		@Override
		public Statement toStatement() {
			return delegate.toStatement();
		}

	}

	interface Processor<T> {
		T process(ResultSet resultSet);
	}

	final class ProcessingQueryOperation<T> extends ForwardingQueryOperation<T> {

		private final Processor<T> processor;

		ProcessingQueryOperation(QueryOperation delegate, Processor<T> processor) {
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

	final class DefaultSingleResultQueryOperation extends AbstractSingleResultQueryOperation {

		private final DefaultQueryOperation defaultQueryOperation;
		private final boolean expectedSingleResult;

		DefaultSingleResultQueryOperation(DefaultQueryOperation defaultQueryOperation, boolean singleResult) {
			super(defaultQueryOperation.cqlTemplate, defaultQueryOperation.statement, singleResult);
			this.defaultQueryOperation = defaultQueryOperation;
			this.expectedSingleResult = singleResult;
		}

		@Override
		public <R> TransformOperation<R> map(final RowMapper<R> rowMapper) {

			return new ProcessingQueryOperation<R>(defaultQueryOperation, new Processor<R>() {

				@Override
				public R process(ResultSet resultSet) {
					return cqlTemplate.processOne(resultSet, rowMapper, expectedSingleResult);
				}

			});

		}

		@Override
		public <E> TransformOperation<E> firstColumn(final Class<E> elementType) {

			return new ProcessingQueryOperation<E>(defaultQueryOperation, new Processor<E>() {

				@Override
				public E process(ResultSet resultSet) {
					return cqlTemplate.processOneFirstColumn(resultSet, elementType, expectedSingleResult);
				}

			});
		}

		@Override
		public TransformOperation<Map<String, Object>> map() {

			return new ProcessingQueryOperation<Map<String, Object>>(defaultQueryOperation,
					new Processor<Map<String, Object>>() {

						@Override
						public Map<String, Object> process(ResultSet resultSet) {
							return cqlTemplate.processOneAsMap(resultSet, expectedSingleResult);
						}

					});
		}

	}
}
