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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;

/**
 * Default Ingest operation implementation
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultIngestOperation extends AbstractQueryOperation<List<ResultSet>, IngestOperation> implements
		IngestOperation {

	private final Iterator<Query> queryIterator;

	public DefaultIngestOperation(CqlTemplate cqlTemplate, Iterator<Query> iterator) {
		super(cqlTemplate);
		this.queryIterator = iterator;
	}

	@Override
	public List<ResultSet> execute() {
		return doExecute(queryIterator);
	}

	@Override
	public CassandraFuture<List<ResultSet>> executeAsync() {
		return doExecuteAsync(queryIterator);
	}

	@Override
	public void executeAsync(CallbackHandler<List<ResultSet>> cb) {
		doExecuteAsync(queryIterator, cb);
	}

	@Override
	public List<ResultSet> executeNonstop(int timeoutMls) throws TimeoutException {
		return doExecuteNonstop(queryIterator, timeoutMls);
	}

}
