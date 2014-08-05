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

import org.springdata.cql.core.RowMapper;
import org.springframework.data.convert.EntityReader;
import org.springframework.util.Assert;

import com.datastax.driver.core.Row;

/**
 * Row mapper that converts Row to Entity
 * 
 * @author Alex Shvid
 * 
 * @param <T>
 */
public class ReaderRowMapper<T> implements RowMapper<T> {

	private final EntityReader<? super T, Object> reader;
	private final Class<T> entityClass;

	public ReaderRowMapper(EntityReader<? super T, Object> reader, Class<T> entityClass) {
		Assert.notNull(reader);
		Assert.notNull(entityClass);
		this.reader = reader;
		this.entityClass = entityClass;
	}

	@Override
	public T mapRow(Row row, int rowNum) {
		T source = reader.read(entityClass, row);
		return source;
	}

}
