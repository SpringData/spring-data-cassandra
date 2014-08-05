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

import org.springdata.cql.core.ConsistencyLevel;
import org.springdata.cql.core.RetryPolicy;
import org.springframework.data.repository.query.ParametersParameterAccessor;

/**
 * Cassandra specific {@link ParametersParameterAccessor}.
 * 
 * @author Alex Shvid
 */
public class CassandraParametersParameterAccessor extends ParametersParameterAccessor implements
		CassandraParameterAccessor {

	private final CassandraQueryMethod method;

	/**
	 * Creates a new {@link CassandraParametersParameterAccessor}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param values must not be {@@iteral null}.
	 */
	public CassandraParametersParameterAccessor(CassandraQueryMethod method, Object[] values) {
		super(method.getParameters(), values);
		this.method = method;
	}

	@Override
	public ConsistencyLevel getConsistencyLevel() {
		int index = method.getParameters().getConsistencyLevelIndex();
		return index == -1 ? null : (ConsistencyLevel) getValue(index);
	}

	@Override
	public RetryPolicy getRetryPolicy() {
		int index = method.getParameters().getRetryPolicyIndex();
		return index == -1 ? null : (RetryPolicy) getValue(index);
	}

	@Override
	public Boolean getQueryTracing() {
		int index = method.getParameters().getQueryTracingIndex();
		if (index == -1) {
			return null;
		}
		Object value = getValue(index);
		if (value == null) {
			return null;
		}
		if (!(value instanceof Boolean)) {
			throw new IllegalArgumentException("value in index " + index + " must be Boolean");
		}
		return (Boolean) value;
	}

	@Override
	public Integer getTtl() {
		int index = method.getParameters().getTtlIndex();
		if (index == -1) {
			return null;
		}
		Object value = getValue(index);
		if (value == null) {
			return null;
		}
		if (!(value instanceof Integer)) {
			throw new IllegalArgumentException("value in index " + index + " must be Integer");
		}
		return (Integer) value;
	}

	@Override
	public Long getTimestamp() {
		int index = method.getParameters().getTimestampIndex();
		if (index == -1) {
			return null;
		}
		Object value = getValue(index);
		if (value == null) {
			return null;
		}
		if (!(value instanceof Long)) {
			throw new IllegalArgumentException("value in index " + index + " must be Long");
		}
		return (Long) value;
	}

}
