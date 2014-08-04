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
package org.springdata.cassandra.cql.support.exception;

import org.springframework.dao.DataIntegrityViolationException;

import com.datastax.driver.core.ResultSet;

/**
 * 
 * @author Alex Shvid
 * 
 */
public class CassandraNotSingleResultException extends DataIntegrityViolationException {

	private static final long serialVersionUID = 9130965082234425995L;

	private final ResultSet resultSet;

	public CassandraNotSingleResultException(ResultSet resultSet) {
		super("expected single row in the resultSet");
		this.resultSet = resultSet;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

}
