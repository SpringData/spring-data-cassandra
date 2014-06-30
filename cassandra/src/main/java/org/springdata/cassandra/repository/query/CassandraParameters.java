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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.springdata.cassandra.cql.core.ConsistencyLevel;
import org.springdata.cassandra.cql.core.RetryPolicy;
import org.springframework.core.MethodParameter;
import org.springframework.data.repository.query.Parameters;

/**
 * Custom extension of {@link Parameters} for Cassandra
 * 
 * @author Alex Shvid
 */
public class CassandraParameters extends Parameters<CassandraParameters, CassandraParameter> {

	private final int consistencyLevelIndex;
	private final int retryPolicyIndex;
	private int queryTracingIndex = 1;
	private int ttlIndex = -1;
	private int timestampIndex = -1;

	/**
	 * Creates a new {@link CassandraParameters} instance from the given {@link Method} and {@link CassandraQueryMethod}.
	 * 
	 * @param method must not be {@literal null}.
	 */
	public CassandraParameters(Method method) {
		super(method);

		List<Class<?>> parameterTypes = Arrays.asList(method.getParameterTypes());
		this.consistencyLevelIndex = parameterTypes.indexOf(ConsistencyLevel.class);
		this.retryPolicyIndex = parameterTypes.indexOf(RetryPolicy.class);
	}

	private CassandraParameters(List<CassandraParameter> parameters, CassandraParameters other) {
		super(parameters);
		this.consistencyLevelIndex = other.consistencyLevelIndex;
		this.retryPolicyIndex = other.retryPolicyIndex;
		this.queryTracingIndex = other.queryTracingIndex;
		this.ttlIndex = other.ttlIndex;
		this.timestampIndex = other.timestampIndex;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createParameter(org.springframework.core.MethodParameter)
	 */
	@Override
	protected CassandraParameter createParameter(MethodParameter parameter) {

		CassandraParameter cassandraParameter = new CassandraParameter(parameter);

		// Detect manually annotated @QueryTracing parameter and reject multiple annotated ones
		if (this.queryTracingIndex == -1 && cassandraParameter.isQueryTracing()) {
			this.queryTracingIndex = cassandraParameter.getIndex();
		} else if (cassandraParameter.isQueryTracing()) {
			throw new IllegalStateException(String.format(
					"Found multiple @QueryTracing annotations on method %s! Only one allowed!", parameter.getMethod().toString()));
		}

		// Detect manually annotated @Ttl parameter and reject multiple annotated ones
		if (this.ttlIndex == -1 && cassandraParameter.isTtl()) {
			this.ttlIndex = cassandraParameter.getIndex();
		} else if (cassandraParameter.isTtl()) {
			throw new IllegalStateException(String.format("Found multiple @Ttl annotations on method %s! Only one allowed!",
					parameter.getMethod().toString()));
		}

		// Detect manually annotated @Timestamp parameter and reject multiple annotated ones
		if (this.timestampIndex == -1 && cassandraParameter.isTimestamp()) {
			this.timestampIndex = cassandraParameter.getIndex();
		} else if (cassandraParameter.isTimestamp()) {
			throw new IllegalStateException(String.format(
					"Found multiple @Timestamp annotations on method %s! Only one allowed!", parameter.getMethod().toString()));
		}

		return cassandraParameter;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createFrom(java.util.List)
	 */
	@Override
	protected CassandraParameters createFrom(List<CassandraParameter> parameters) {
		return new CassandraParameters(parameters, this);
	}

	public int getConsistencyLevelIndex() {
		return consistencyLevelIndex;
	}

	public int getRetryPolicyIndex() {
		return retryPolicyIndex;
	}

	public int getQueryTracingIndex() {
		return queryTracingIndex;
	}

	public int getTtlIndex() {
		return ttlIndex;
	}

	public int getTimestampIndex() {
		return timestampIndex;
	}

}
