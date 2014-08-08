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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

/**
 * Simple query creator
 * 
 * @author Alex Shvid
 * 
 */

public class SimpleQueryCreator implements QueryCreator {

	private final Statement statement;

	public SimpleQueryCreator(String cql) {
		statement = new SimpleStatement(cql);
	}

	public SimpleQueryCreator(Statement query) {
		statement = query;
	}

	public SimpleQueryCreator withConsistencyLevel(ConsistencyLevel consistency) {
		if (consistency != null) {
			statement.setConsistencyLevel(consistency);
		}
		return this;
	}

	public SimpleQueryCreator withRetryPolicy(RetryPolicy retryPolicy) {
		if (retryPolicy != null) {
			statement.setRetryPolicy(RetryPolicyResolver.resolve(retryPolicy));
		}
		return this;
	}

	public SimpleQueryCreator enableTracing() {
		statement.enableTracing();
		return this;
	}

	public SimpleQueryCreator disableTracing() {
		statement.disableTracing();
		return this;
	}

	public SimpleQueryCreator withQueryTracing(Boolean queryTracing) {
		if (queryTracing != null) {
			if (queryTracing.booleanValue()) {
				statement.enableTracing();
			} else {
				statement.disableTracing();
			}
		}
		return this;
	}

	@Override
	public Statement createQuery() {
		return statement;
	}

}
