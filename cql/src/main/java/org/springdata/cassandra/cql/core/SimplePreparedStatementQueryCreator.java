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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;

/**
 * Prepared Statement Query Creator
 * 
 * @author Alex Shvid
 * 
 */
public class SimplePreparedStatementQueryCreator implements QueryCreator {

	private final PreparedStatement ps;
	private PreparedStatementBinder psbOrNull;

	private ConsistencyLevel consistency;
	private RetryPolicy retryPolicy;
	private Boolean queryTracing;

	public SimplePreparedStatementQueryCreator(PreparedStatement ps) {
		this.ps = ps;
	}

	public SimplePreparedStatementQueryCreator(PreparedStatement ps, PreparedStatementBinder psb) {
		this.ps = ps;
		this.psbOrNull = psb;
	}

	public SimplePreparedStatementQueryCreator withConsistencyLevel(ConsistencyLevel consistency) {
		this.consistency = consistency;
		return this;
	}

	public SimplePreparedStatementQueryCreator withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	public SimplePreparedStatementQueryCreator enableTracing() {
		this.queryTracing = Boolean.TRUE;
		return this;
	}

	public SimplePreparedStatementQueryCreator disableTracing() {
		this.queryTracing = Boolean.FALSE;
		return this;
	}

	public SimplePreparedStatementQueryCreator withQueryTracing(Boolean queryTracing) {
		this.queryTracing = queryTracing;
		return this;
	}

	@Override
	public Query createQuery() {

		BoundStatement bs = null;
		if (psbOrNull != null) {
			bs = psbOrNull.bindValues(ps);
		} else {
			bs = ps.bind();
		}

		if (consistency != null) {
			bs.setConsistencyLevel(ConsistencyLevelResolver.resolve(consistency));
		}
		if (retryPolicy != null) {
			bs.setRetryPolicy(RetryPolicyResolver.resolve(retryPolicy));
		}
		if (queryTracing != null) {
			if (queryTracing.booleanValue()) {
				bs.enableTracing();
			} else {
				bs.disableTracing();
			}
		}

		return bs;
	}

}
