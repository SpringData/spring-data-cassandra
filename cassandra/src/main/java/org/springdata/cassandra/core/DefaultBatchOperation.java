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
package org.springdata.cassandra.core;

import java.util.Iterator;

import org.springdata.cql.core.AbstractExecuteOperation;
import org.springdata.cql.core.SessionCallback;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Implementation of the BatchOperation. Creates Batch query for save, saveNew, delete, deleteById operations.
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultBatchOperation extends AbstractExecuteOperation<BatchOperation> implements BatchOperation {

	private final CassandraTemplate cassandraTemplate;
	private String tableName;
	private Iterator<BatchedStatementCreator> iterator;

	protected DefaultBatchOperation(CassandraTemplate cassandraTemplate, Iterator<BatchedStatementCreator> iterator) {
		super(cassandraTemplate.cqlTemplate());
		this.cassandraTemplate = cassandraTemplate;
		this.iterator = iterator;
	}

	@Override
	public BatchOperation inTable(String tableName) {
		this.tableName = tableName;
		return this;
	}

	@Override
	public Statement createStatement() {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;

		while (iterator.hasNext()) {

			BatchedStatementCreator bsc = iterator.next();
			RegularStatement statement = doCreateStatement(bsc);
			batch.add(statement);

			emptyBatch = false;
		}

		if (emptyBatch) {
			throw new IllegalArgumentException("entities are empty");
		}

		return batch;
	}

	private RegularStatement doCreateStatement(final BatchedStatementCreator bsc) {

		if (tableName != null) {
			bsc.setTableName(tableName);
		}

		return cassandraTemplate.cqlTemplate().execute(new SessionCallback<RegularStatement>() {

			@Override
			public RegularStatement doInSession(Session session) {
				return bsc.createRegularStatement();
			}

		});
	}

}
