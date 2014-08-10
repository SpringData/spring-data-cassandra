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

import org.springdata.cql.core.StatementOperation;

import com.datastax.driver.core.ResultSet;

/**
 * 
 * Base interface to save new entity (actually insert).
 * 
 * @author Alex Shvid
 * 
 */
public interface SaveNewOperation extends StatementOperation<ResultSet, SaveNewOperation> {

	/**
	 * Specifies table differ from entitie's table to save
	 * 
	 * @param tableName table is using to save entity
	 * @return this
	 */
	SaveNewOperation toTable(String tableName);

	/**
	 * Specifies TTL (time to live) in seconds for the saved entity in the Cassandra
	 * 
	 * @param ttlSeconds Time to live in seconds
	 * @return this
	 */
	SaveNewOperation withTimeToLive(int ttlSeconds);

	/**
	 * Specifies Timestamp (cell's timestamp in the Cassandra) in milliseconds for the saved entity in the Cassandra
	 * 
	 * @param timestamp Timestamp in milliseconds
	 * @return this
	 */
	SaveNewOperation withTimestamp(long timestampMls);

}
