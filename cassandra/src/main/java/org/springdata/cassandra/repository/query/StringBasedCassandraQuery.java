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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.core.CassandraOperations;

import com.datastax.driver.core.Statement;

/**
 * Query to use a plain CQL String to create the {@link Statement} to actually execute.
 * 
 * @author Alex Shvid
 */
public class StringBasedCassandraQuery extends AbstractCassandraQuery {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private static final Pattern PLACEHOLDER = Pattern.compile("\\?(\\d+)");

	private static final Pattern TABLEHOLDER = Pattern.compile("\\?(table)");

	private final String query;
	private final boolean isCountQuery;

	/**
	 * Creates a new {@link StringBasedCassandraQuery}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public StringBasedCassandraQuery(String query, CassandraQueryMethod method, CassandraOperations cassandraOperations) {

		super(method, cassandraOperations);

		this.query = query;
		this.isCountQuery = method.hasAnnotatedQuery() ? method.getQueryAnnotation().count() : false;
	}

	public StringBasedCassandraQuery(CassandraQueryMethod method, CassandraOperations cassandraOperations) {
		this(method.getAnnotatedQuery(), method, cassandraOperations);
	}

	@Override
	protected String createQuery(CassandraParameterAccessor accessor, String tableName) {

		String queryString = replaceTable(query, tableName);

		queryString = replacePlaceholders(queryString, accessor);

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Created query %s", queryString));
		}

		return queryString;
	}

	@Override
	protected boolean isCountQuery() {
		return isCountQuery;
	}

	private String replacePlaceholders(String input, CassandraParameterAccessor accessor) {

		Matcher matcher = PLACEHOLDER.matcher(input);
		String result = input;

		while (matcher.find()) {
			String group = matcher.group();
			int index = Integer.parseInt(matcher.group(1));
			result = result.replace(group, getParameterWithIndex(accessor, index));
		}

		return result;
	}

	private String replaceTable(String input, String tableName) {

		Matcher matcher = TABLEHOLDER.matcher(input);
		return matcher.replaceFirst(input);

	}

	private String getParameterWithIndex(CassandraParameterAccessor accessor, int index) {
		Object obj = accessor.getBindableValue(index);

		//
		// TODO: convert Object parameter to String query parameter
		//

		return obj != null ? obj.toString() : "";
	}
}
