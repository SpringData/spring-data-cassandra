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

import org.springdata.cql.core.AbstractExecuteOperation;
import org.springdata.cql.core.QueryOperation;
import org.springframework.util.Assert;

import com.datastax.driver.core.ResultSet;

/**
 * Abstract save operation implementation
 * 
 * @author Alex Shvid
 * 
 * @param <T> - entity Type
 * @param <O> - operation Type
 */

public abstract class AbstractSaveOperation<T, O extends QueryOperation<ResultSet, O>> extends
		AbstractExecuteOperation<O> implements BatchedStatementCreator {

	protected final CassandraTemplate cassandraTemplate;
	protected final T entity;
	private String tableName;
	private Integer ttl;
	private Long timestamp;

	protected AbstractSaveOperation(CassandraTemplate cassandraTemplate, T entity) {
		super(cassandraTemplate.cqlTemplate());
		Assert.notNull(cassandraTemplate);
		Assert.notNull(entity);
		this.cassandraTemplate = cassandraTemplate;
		this.entity = entity;
	}

	public String getTableName() {
		return tableName != null ? tableName : cassandraTemplate.getTableName(entity.getClass());
	}

	@Override
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setTtl(Integer ttl) {
		this.ttl = ttl;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Integer getTtl() {
		return ttl;
	}

	public Long getTimestamp() {
		return timestamp;
	}

}
