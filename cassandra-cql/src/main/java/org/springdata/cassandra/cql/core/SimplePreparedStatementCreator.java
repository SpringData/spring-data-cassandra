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
package org.springdata.cassandra.cql.core;

import org.springframework.util.Assert;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * @author David Webb
 * @author Alex Shvid
 * 
 */
public class SimplePreparedStatementCreator implements PreparedStatementCreator {

	private final String cql;

	private ConsistencyLevel consistency;
	private RetryPolicy retryPolicy;
	private Boolean queryTracing;

	/**
	 * Create a PreparedStatementCreator from the provided CQL.
	 * 
	 * @param cql
	 */
	public SimplePreparedStatementCreator(String cql) {
		Assert.notNull(cql, "CQL is required to create a PreparedStatement");
		this.cql = cql;
	}

	public SimplePreparedStatementCreator withConsistencyLevel(ConsistencyLevel consistency) {
		this.consistency = consistency;
		return this;
	}

	public SimplePreparedStatementCreator withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	public SimplePreparedStatementCreator enableTracing() {
		this.queryTracing = Boolean.TRUE;
		return this;
	}

	public SimplePreparedStatementCreator disableTracing() {
		this.queryTracing = Boolean.FALSE;
		return this;
	}

	public SimplePreparedStatementCreator withQueryTracing(Boolean queryTracing) {
		this.queryTracing = queryTracing;
		return this;
	}

	public String getCql() {
		return this.cql;
	}

	@Override
	public PreparedStatement createPreparedStatement(Session session) {

		PreparedStatement ps = session.prepare(this.cql);

		if (consistency != null) {
			ps.setConsistencyLevel(ConsistencyLevelResolver.resolve(consistency));
		}
		if (retryPolicy != null) {
			ps.setRetryPolicy(RetryPolicyResolver.resolve(retryPolicy));
		}
		if (queryTracing != null) {
			if (queryTracing.booleanValue()) {
				ps.enableTracing();
			} else {
				ps.disableTracing();
			}
		}

		return ps;
	}

}
