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

import com.datastax.driver.core.Query;

/**
 * Default update operation implementation
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultExecuteOperation extends AbstractExecuteOperation<ExecuteOperation> implements ExecuteOperation {

	private final QueryCreator qc;

	public DefaultExecuteOperation(CqlTemplate cqlTemplate, String cql) {
		this(cqlTemplate, new SimpleQueryCreator(cql));
	}

	public DefaultExecuteOperation(CqlTemplate cqlTemplate, Query query) {
		this(cqlTemplate, new SimpleQueryCreator(query));
	}

	public DefaultExecuteOperation(CqlTemplate cqlTemplate, QueryCreator qc) {
		super(cqlTemplate);
		this.qc = qc;
	}

	@Override
	public Query createQuery() {
		return qc.createQuery();
	}

}