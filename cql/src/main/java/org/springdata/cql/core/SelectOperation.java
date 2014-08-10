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
package org.springdata.cql.core;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;

/**
 * General class for select operations. Support transformation and mapping of the ResultSet
 * 
 * @author Alex Shvid
 * 
 */

public interface SelectOperation extends StatementOperation<ResultSet, SelectOperation> {

	/**
	 * Returns single result operation
	 * 
	 * @return SelectOneOperation
	 */
	SelectOneOperation firstRow();

	/**
	 * Returns single result operation
	 * 
	 * @return SelectOneOperation
	 */
	SelectOneOperation singleResult();

	/**
	 * Maps each row in ResultSet by RowMapper.
	 * 
	 * @param rowMapper
	 * @return ProcessOperation
	 */
	<R> ProcessOperation<List<R>> map(RowMapper<R> rowMapper);

	/**
	 * Returns true if ResultSet is not empty.
	 * 
	 * @return ProcessOperation
	 */
	ProcessOperation<Boolean> exists();

	/**
	 * Retrieves only the first column from ResultSet, expected type is elementType class.
	 * 
	 * @param elementType
	 * @return ProcessOperation
	 */
	<E> ProcessOperation<List<E>> firstColumn(Class<E> elementType);

	/**
	 * Maps all rows from ResultSet to Map<String, Object>.
	 * 
	 * @return ProcessOperation
	 */
	ProcessOperation<List<Map<String, Object>>> map();

	/**
	 * Uses ResultSetCallback to transform ResultSet to object with type T.
	 * 
	 * @param rse ResultSet Extractor to convert ResultSet to type T
	 * @return ProcessOperation
	 */
	<O> ProcessOperation<O> transform(ResultSetExtractor<O> rse);

	/**
	 * Calls RowCallbackHandler for each row in ResultSet.
	 * 
	 * @param rch
	 * @return ProcessOperation
	 */
	ProcessOperation<Object> forEach(RowCallbackHandler rch);

}
