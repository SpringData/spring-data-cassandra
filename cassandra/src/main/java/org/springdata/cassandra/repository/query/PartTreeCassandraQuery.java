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

import org.springdata.cassandra.core.CassandraOperations;
import org.springdata.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * PartTree {@link RepositoryQuery} implementation for Cassandra.
 * 
 * @author Alex Shvid
 */
public class PartTreeCassandraQuery extends AbstractCassandraQuery {

	private final PartTree tree;
	private final MappingContext<?, CassandraPersistentProperty> context;

	/**
	 * Creates a new {@link PartTreeCassandraQuery} from the given {@link QueryMethod} and {@link CassandraTemplate}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public PartTreeCassandraQuery(CassandraQueryMethod method, CassandraOperations cassandraOperations) {

		super(method, cassandraOperations);
		this.tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());
		this.context = cassandraOperations.getConverter().getMappingContext();
	}

	/**
	 * Return the {@link PartTree} backing the query.
	 * 
	 * @return the tree
	 */
	public PartTree getTree() {
		return tree;
	}

	@Override
	protected String createQuery(CassandraParameterAccessor accessor, String tableName) {
		CassandraQueryCreator creator = new CassandraQueryCreator(tree, accessor, context, tableName, false);
		return creator.createQuery().getQueryString();
	}

	@Override
	protected String createCountQuery(CassandraParameterAccessor accessor, String tableName) {
		CassandraQueryCreator creator = new CassandraQueryCreator(tree, accessor, context, tableName, true);
		return creator.createQuery().getQueryString();
	}

	@Override
	protected boolean isCountQuery() {
		return tree.isCountProjection();
	}

}
