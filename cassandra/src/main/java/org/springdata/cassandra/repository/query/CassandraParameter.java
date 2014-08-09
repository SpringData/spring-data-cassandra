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

import org.springdata.cassandra.repository.QueryTracing;
import org.springdata.cassandra.repository.TimeToLiveSeconds;
import org.springdata.cassandra.repository.TimestampMilliseconds;
import org.springframework.core.MethodParameter;
import org.springframework.data.repository.query.Parameter;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Custom {@link Parameter} implementation for Cassandra.
 * 
 * @author Alex Shvid
 */
public class CassandraParameter extends Parameter {

	private final MethodParameter parameter;

	/**
	 * Creates a new {@link CassandraParameter}.
	 * 
	 * @param parameter must not be {@literal null}.
	 */
	public CassandraParameter(MethodParameter parameter) {
		super(parameter);
		this.parameter = parameter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameter#isSpecialParameter()
	 */
	@Override
	public boolean isSpecialParameter() {
		return super.isSpecialParameter() || isConsistencyLevel() || isRetryPolicy() || isManuallyAnnotatedParameter();
	}

	boolean isManuallyAnnotatedParameter() {
		return hasQueryTracingAnnotation() || hasTtlAnnotation() || hasTimestampAnnotation();
	}

	boolean isConsistencyLevel() {
		return getType().equals(ConsistencyLevel.class);
	}

	boolean isRetryPolicy() {
		return getType().equals(RetryPolicy.class);
	}

	boolean isQueryTracing() {
		return (getType().equals(Boolean.class) || getType().equals(boolean.class)) && hasQueryTracingAnnotation();
	}

	boolean isTtl() {
		return (getType().equals(Integer.class) || getType().equals(int.class)) && hasTtlAnnotation();
	}

	boolean isTimestamp() {
		return (getType().equals(Long.class) || getType().equals(long.class)) && hasTimestampAnnotation();
	}

	boolean hasQueryTracingAnnotation() {
		return parameter.getParameterAnnotation(QueryTracing.class) != null;
	}

	boolean hasTtlAnnotation() {
		return parameter.getParameterAnnotation(TimeToLiveSeconds.class) != null;
	}

	boolean hasTimestampAnnotation() {
		return parameter.getParameterAnnotation(TimestampMilliseconds.class) != null;
	}

}
