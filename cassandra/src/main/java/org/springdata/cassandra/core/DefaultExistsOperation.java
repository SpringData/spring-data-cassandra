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
import java.util.LinkedList;
import java.util.List;

import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Exists operation implementation
 * 
 * @author Alex Shvid
 * 
 * @param <T> - entity by the type T
 */

public class DefaultExistsOperation<T> extends AbstractGetOperation<Boolean> {

	private final CassandraTemplate cassandraTemplate;
	private final T entity;
	private final Class<T> entityClass;
	private final Object id;

	public DefaultExistsOperation(CassandraTemplate cassandraTemplate, T entity) {
		super(cassandraTemplate.cqlTemplate());
		Assert.notNull(entity);
		this.cassandraTemplate = cassandraTemplate;
		this.entity = entity;
		this.entityClass = null;
		this.id = null;
	}

	public DefaultExistsOperation(CassandraTemplate cassandraTemplate, Class<T> entityClass, Object id) {
		super(cassandraTemplate.cqlTemplate());
		Assert.notNull(entityClass);
		Assert.notNull(id);
		this.cassandraTemplate = cassandraTemplate;
		this.entity = null;
		this.entityClass = entityClass;
		this.id = id;
	}

	@Override
	public Boolean transform(ResultSet resultSet) {
		Iterator<Row> i = resultSet.iterator();
		if (i.hasNext()) {
			Row row = i.next();
			long count = row.getLong(0);
			return count > 0;
		} else {
			return false;
		}
	}

	@Override
	public Query createQuery() {

		String tableName = getTableName();
		if (tableName == null) {
			tableName = cassandraTemplate.getTableName(entity != null ? entity.getClass() : entityClass);
		}

		Select select = QueryBuilder.select().countAll().from(cassandraTemplate.getKeyspace(), tableName);
		Select.Where w = select.where();

		CassandraPersistentEntity<?> persistentEntity = cassandraTemplate.getPersistentEntity(entity != null ? entity
				.getClass() : entityClass);

		List<Clause> clauseList = null;

		if (entity != null) {
			clauseList = new LinkedList<Clause>();
			cassandraTemplate.getConverter().write(entity, clauseList);
		} else {
			clauseList = cassandraTemplate.getConverter().getPrimaryKey(persistentEntity, id);
		}

		for (Clause c : clauseList) {
			w.and(c);
		}

		return select;
	}
}
