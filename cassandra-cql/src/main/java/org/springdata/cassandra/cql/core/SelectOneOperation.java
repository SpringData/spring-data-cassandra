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

import java.util.Map;

import com.datastax.driver.core.Row;

/**
 * SelectOneOperation is used for single result.
 * 
 * @author Alex Shvid
 * 
 */
public interface SelectOneOperation extends QueryOperation<Row, SelectOneOperation> {

	/**
	 * Maps first row in ResultSet by RowMapper.
	 * 
	 * @param rowMapper
	 * @return ProcessOperation
	 */
	<R> ProcessOperation<R> map(RowMapper<R> rowMapper);

	/**
	 * Retrieves first row in the first column, expected type is elementType class.
	 * 
	 * @param elementType
	 * @return ProcessOperation
	 */
	<E> ProcessOperation<E> firstColumn(Class<E> elementType);

	/**
	 * Maps only first row from ResultSet to Map<String, Object>.
	 * 
	 * @return ProcessOperation
	 */
	ProcessOperation<Map<String, Object>> map();

}
