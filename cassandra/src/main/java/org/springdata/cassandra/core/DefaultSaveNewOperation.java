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

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Implementation for SaveNewOperation
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultSaveNewOperation<T> extends AbstractSaveOperation<T, SaveNewOperation> implements SaveNewOperation {

	protected DefaultSaveNewOperation(CassandraTemplate cassandraTemplate, T entity) {
		super(cassandraTemplate, entity);
	}

	@Override
	public Statement createQuery() {
		return createStatement();
	}

	@Override
	public RegularStatement createStatement() {

		Insert query = QueryBuilder.insertInto(cassandraTemplate.getKeyspace(), getTableName());

		cassandraTemplate.getConverter().write(entity, query);

		/*
		 * Add Ttl and Timestamp to Insert query
		 */
		if (getTtl() != null) {
			query.using(QueryBuilder.ttl(getTtl()));
		}
		if (getTimestamp() != null) {
			query.using(QueryBuilder.timestamp(getTimestamp()));
		}

		return query;
	}

	@Override
	public SaveNewOperation toTable(String tableName) {
		setTableName(tableName);
		return this;
	}

	@Override
	public SaveNewOperation withTimeToLive(int ttlSeconds) {
		setTtl(ttlSeconds);
		return this;
	}

	@Override
	public SaveNewOperation withTimestamp(long timestampMls) {
		setTimestamp(timestampMls);
		return this;
	}

}
