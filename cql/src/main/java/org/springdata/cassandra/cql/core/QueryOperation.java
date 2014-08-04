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
package org.springdata.cassandra.cql.core;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Query;

/**
 * Cassandra Query Operation
 * 
 * @author Alex Shvid
 * 
 */
public interface QueryOperation<T, O extends QueryOperation<T, O>> {

	/**
	 * Adds consistency level to the query operation
	 * 
	 * @param consistencyLevel ConsistencyLevel
	 * @return this
	 */
	O withConsistencyLevel(ConsistencyLevel consistencyLevel);

	/**
	 * Adds retry policy to the query operation
	 * 
	 * @param retryPolicy RetryPolicy
	 * @return this
	 */
	O withRetryPolicy(RetryPolicy retryPolicy);

	/**
	 * Adds query tracing option to the query operation
	 * 
	 * @param queryTracing Boolean to enable/disable query tracing
	 * @return this
	 */
	O withQueryTracing(Boolean queryTracing);

	/**
	 * Uses fallback handler to send errors in asynchronous execution.
	 * 
	 * @param fh
	 * @return this
	 */
	O withFallbackHandler(FallbackHandler fh);

	/**
	 * Specifies Executor that will be used for asynchronous execution. By default will be used SameThreadExecutor that
	 * will be the thread that calls ResultSetFuture.set() and it will be Datastax Driver internal thread. It is
	 * recommended to specify application thread pool executor for asynchronous calls.
	 * 
	 * @param executor Executor service
	 * @return this
	 */
	O withExecutor(Executor executor);

	/**
	 * Synchronously executes query operation and returns object with type T
	 * 
	 * @return object with type T
	 */
	T execute();

	/**
	 * Asynchronously executes query operation and returns Future of the object with type T
	 * 
	 * @return Future of the object with type T
	 */
	CassandraFuture<T> executeAsync();

	/**
	 * Asynchronously executes query operation and call CallbackHandler with result on completion
	 * 
	 * @return ResultSetFuture
	 */
	void executeAsync(CallbackHandler<T> cb);

	/**
	 * Synchronously executes select operation for the given time interval in milliseconds and returns object with type T.
	 * Useful for SLA services that guarantees response time.
	 * 
	 * @return object with type T or throws TimeoutException if execution time is grater than expected. Actually does not
	 *         cancel operation on Cassandra servers due to unsupported feature in Cassandra itself.
	 * 
	 */
	T executeNonstop(int timeoutMls) throws TimeoutException;

	/**
	 * Builds Cql Query for current operation
	 * 
	 * @return cql Query
	 */

	Query toQuery();

}
