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

import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Count operation implementation
 * 
 * @author Alex Shvid
 * 
 * @param <T> - entity by the type T
 */

public class DefaultCountOperation<T> extends AbstractGetOperation<Long> {

	private final CassandraTemplate cassandraTemplate;
	private final Class<T> entityClass;

	public DefaultCountOperation(CassandraTemplate cassandraTemplate, Class<T> entityClass) {
		super(cassandraTemplate.cqlTemplate());
		Assert.notNull(entityClass);
		this.cassandraTemplate = cassandraTemplate;
		this.entityClass = entityClass;
	}

	@Override
	public Long transform(ResultSet resultSet) {
		Iterator<Row> i = resultSet.iterator();
		if (i.hasNext()) {
			Row row = i.next();
			long count = row.getLong(0);
			return count;
		} else {
			return 0L;
		}
	}

	@Override
	public Statement createQuery() {

		String tableName = getTableName();
		if (tableName == null) {
			tableName = cassandraTemplate.getTableName(entityClass);
		}

		Select select = QueryBuilder.select().countAll().from(cassandraTemplate.getKeyspace(), tableName);
		return select;
	}

}
