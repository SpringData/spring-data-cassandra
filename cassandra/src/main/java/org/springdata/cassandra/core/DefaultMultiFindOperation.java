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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springdata.cassandra.cql.core.QueryCreator;
import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.convert.EntityReader;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Default Find by Ids operation
 * 
 * @author Alex Shvid
 * 
 */

public class DefaultMultiFindOperation<T> extends AbstractMultiGetOperation<List<T>> {

	private final CassandraTemplate cassandraTemplate;
	private final EntityReader<? super T, Object> entityReader;
	private final Class<T> entityClass;
	private final CassandraPersistentEntity<?> entity;
	private final Iterator<?> ids;

	public DefaultMultiFindOperation(CassandraTemplate cassandraTemplate, Class<T> entityClass, Iterator<?> ids) {
		super(cassandraTemplate.cqlTemplate());
		this.cassandraTemplate = cassandraTemplate;
		this.entityReader = cassandraTemplate.getConverter();
		this.entityClass = entityClass;
		this.entity = cassandraTemplate.getPersistentEntity(entityClass);
		this.ids = ids;
	}

	@Override
	public Iterator<Query> getQueryIterator() {

		final String tableName = getTableName() != null ? getTableName() : entity.getTableName();

		return Iterators.transform(ids, new Function<Object, Query>() {

			@Override
			public Query apply(final Object id) {
				return cassandraTemplate.cqlOps().createQuery(new QueryCreator() {

					@Override
					public Query createQuery() {

						Select select = QueryBuilder.select().all().from(cassandraTemplate.getKeyspace(), tableName);
						Select.Where w = select.where();

						List<Clause> list = cassandraTemplate.getConverter().getPrimaryKey(entity, id);

						for (Clause c : list) {
							w.and(c);
						}

						return select;
					}

				});
			}

		});

	}

	@Override
	public List<T> transform(List<ResultSet> resultSets) {

		List<T> result = new ArrayList<T>(resultSets.size());

		for (ResultSet resultSet : resultSets) {

			Row row = resultSet.one();
			if (row != null) {
				T obj = entityReader.read(entityClass, row);
				result.add(obj);
			}
		}

		return result;
	}

}
