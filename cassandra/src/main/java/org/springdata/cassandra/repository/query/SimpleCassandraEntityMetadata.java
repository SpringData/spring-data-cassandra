/*
 * Copyright 2013 the original author or authors.
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
package org.springdata.cassandra.repository.query;

import org.springframework.util.Assert;

/**
 * Bean based implementation of {@link CassandraEntityMetadata}.
 * 
 * @author Alex Shvid
 */
public class SimpleCassandraEntityMetadata<T> implements CassandraEntityMetadata<T> {

	private final Class<T> type;
	private final String tableName;

	/**
	 * Creates a new {@link SimpleCassandraEntityMetadata} using the given type and table name.
	 * 
	 * @param type must not be {@literal null}.
	 * @param tableName must not be {@literal null} or empty.
	 */
	public SimpleCassandraEntityMetadata(Class<T> type, String tableName) {

		Assert.notNull(type, "Type must not be null!");
		Assert.hasText(tableName, "TableName name must not be null or empty!");

		this.type = type;
		this.tableName = tableName;
	}

	@Override
	public Class<T> getJavaType() {
		return type;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

}
