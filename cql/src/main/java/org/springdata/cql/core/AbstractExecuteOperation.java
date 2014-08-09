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

import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;

/**
 * 
 * @author Alex Shvid
 * 
 * @param <O> Operation type
 */
public abstract class AbstractExecuteOperation<O extends QueryOperation<ResultSet, O>> extends
		AbstractQueryOperation<ResultSet, O> implements QueryCreator {

	protected AbstractExecuteOperation(CqlTemplate cqlTemplate) {
		super(cqlTemplate);
	}

	@Override
	public ResultSet execute() {
		Statement query = doCreateQuery(this);
		return doExecute(query);
	}

	@Override
	public CassandraFuture<ResultSet> executeAsync() {
		Statement query = doCreateQuery(this);
		return doExecuteAsync(query);
	}

	@Override
	public void executeAsync(final CallbackHandler<ResultSet> cb) {
		Statement query = doCreateQuery(this);
		doExecuteAsync(query, cb);
	}

	@Override
	public ResultSet executeNonstop(int timeoutMls) throws TimeoutException {
		Statement query = doCreateQuery(this);
		return doExecuteNonstop(query, timeoutMls);
	}

	@Override
	public Statement toStatement() {
		return doCreateQuery(this);
	}

}
