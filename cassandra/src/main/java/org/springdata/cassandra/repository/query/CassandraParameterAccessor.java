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
package org.springdata.cassandra.repository.query;

import org.springdata.cassandra.cql.core.ConsistencyLevel;
import org.springdata.cassandra.cql.core.RetryPolicy;
import org.springframework.data.repository.query.ParameterAccessor;

/**
 * Cassandra specific {@link ParameterAccessor}.
 * 
 * @author Alex Shvid
 */

public interface CassandraParameterAccessor extends ParameterAccessor {

	/**
	 * Returns ConsistencyLevel for the query operation. By default Cassandra uses ConsistencyLevel.QUORUM. Useful for all
	 * gets and puts operations
	 * 
	 * @return ConsistencyLevel enum
	 */

	ConsistencyLevel getConsistencyLevel();

	/**
	 * Returns RetryPolicy for the query operation. By default Cassandra uses RetryPolicy.DEFAULT. Useful for all gets and
	 * puts operations
	 * 
	 * @return RetryPolicy enum
	 */

	RetryPolicy getRetryPolicy();

	/**
	 * Returns QueryTracing for the query operation.
	 * 
	 * @return QueryTracing boolean
	 */

	Boolean getQueryTracing();

	/**
	 * Returns TTL time in seconds for insert and update operations. It is an expiration time for the stored value in
	 * Cassandra. Usefull for insert, update operations.
	 * 
	 * @return ttl in seconds
	 */

	Integer getTtl();

	/**
	 * Return timestamp long in milliseconds. Each cell in Cassandra has timestamp. Usefull for insert, update, delete
	 * operations.
	 * 
	 * @return
	 */

	Long getTimestamp();

}
