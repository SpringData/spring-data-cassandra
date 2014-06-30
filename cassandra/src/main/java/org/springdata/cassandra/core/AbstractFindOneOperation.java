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

import org.springframework.data.convert.EntityReader;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * Abstract Find One Operation
 * 
 * @author Alex Shvid
 * 
 * @param <T> - return Type
 */
public abstract class AbstractFindOneOperation<T> extends AbstractGetOperation<T> {

	protected final CassandraTemplate cassandraTemplate;
	protected final EntityReader<? super T, Object> entityReader;
	protected final Class<T> entityClass;

	public AbstractFindOneOperation(CassandraTemplate cassandraTemplate, Class<T> entityClass) {
		super(cassandraTemplate.cqlTemplate());
		this.cassandraTemplate = cassandraTemplate;
		this.entityReader = cassandraTemplate.getConverter();
		this.entityClass = entityClass;
	}

	@Override
	public String getTableName() {
		String tableName = super.getTableName();
		if (tableName != null) {
			return tableName;
		}
		return cassandraTemplate.getTableName(entityClass);
	}

	@Override
	public T transform(ResultSet resultSet) {

		Row row = resultSet.one();
		if (row != null) {
			return entityReader.read(entityClass, row);
		}
		return null;

	}

}
