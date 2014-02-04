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
package org.springdata.cassandra.repository.support;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.springdata.cassandra.core.CassandraTemplate;
import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.mapping.CassandraPersistentProperty;
import org.springdata.cassandra.repository.CassandraRepository;
import org.springdata.cassandra.repository.query.CassandraEntityInformation;
import org.springdata.cassandra.repository.query.CassandraQueryMethod;
import org.springdata.cassandra.repository.query.PartTreeCassandraQuery;
import org.springdata.cassandra.repository.query.StringBasedCassandraQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;

/**
 * Factory to create {@link CassandraRepository} instances.
 * 
 * @author Alex Shvid
 * 
 */

public class CassandraRepositoryFactory extends RepositoryFactorySupport {

	private final CassandraTemplate cassandraTemplate;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link CassandraRepositoryFactory} with the given {@link CassandraRepositoryFactory}.
	 * 
	 * @param cassandraTemplate must not be {@literal null}
	 */
	public CassandraRepositoryFactory(CassandraTemplate cassandraTemplate) {

		Assert.notNull(cassandraTemplate);

		this.cassandraTemplate = cassandraTemplate;
		this.mappingContext = cassandraTemplate.getConverter().getMappingContext();
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleCassandraRepository.class;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object getTargetRepository(RepositoryMetadata metadata) {

		CassandraEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());

		return new SimpleCassandraRepository(entityInformation, metadata.getRepositoryInterface(), cassandraTemplate);

	}

	@Override
	protected QueryLookupStrategy getQueryLookupStrategy(Key key) {
		return new CassandraQueryLookupStrategy();
	}

	/**
	 * {@link QueryLookupStrategy} to create {@link StringBasedCassandraQuery} instances.
	 * 
	 * @author Alex Shvid
	 */
	private class CassandraQueryLookupStrategy implements QueryLookupStrategy {

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, NamedQueries namedQueries) {

			CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, metadata, mappingContext);
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new StringBasedCassandraQuery(namedQuery, queryMethod, cassandraTemplate);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new StringBasedCassandraQuery(queryMethod, cassandraTemplate);
			} else {
				return new PartTreeCassandraQuery(queryMethod, cassandraTemplate);
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T, ID extends Serializable> CassandraEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

		if (entity == null) {
			throw new MappingException(String.format("Could not lookup mapping metadata for domain class %s!",
					domainClass.getName()));
		}

		return new MappingCassandraEntityInformation<T, ID>((CassandraPersistentEntity<T>) entity);
	}
}
