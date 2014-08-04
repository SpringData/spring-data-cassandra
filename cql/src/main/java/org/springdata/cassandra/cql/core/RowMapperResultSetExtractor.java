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
package org.springdata.cassandra.cql.core;

import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;

/**
 * ResutSetExtractor that uses RowMapper to extract data
 * 
 * @author Alex Shvid
 * 
 */
public class RowMapperResultSetExtractor<T> implements ResultSetExtractor<List<T>> {

	private final RowMapper<T> rowMapper;

	public RowMapperResultSetExtractor(RowMapper<T> rowMapper) {
		this.rowMapper = rowMapper;
	}

	@Override
	public List<T> extractData(ResultSet resultSet) {

		List<T> result = Lists.newArrayList();

		int rowNum = 0;
		for (Row row : resultSet) {
			T obj = rowMapper.mapRow(row, rowNum++);
			result.add(obj);
		}

		return Collections.unmodifiableList(result);

	}
}
