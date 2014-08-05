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
package org.springdata.cassandra.convert;

import java.util.List;

import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.mapping.CassandraPersistentProperty;
import org.springdata.cql.spec.AlterTableSpecification;
import org.springdata.cql.spec.CreateIndexSpecification;
import org.springdata.cql.spec.CreateTableSpecification;
import org.springdata.cql.spec.WithNameSpecification;
import org.springframework.data.convert.EntityConverter;

import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Clause;

/**
 * Central Cassandra specific converter interface from Object to Row.
 * 
 * @author Alex Shvid
 */
public interface CassandraConverter extends
		EntityConverter<CassandraPersistentEntity<?>, CassandraPersistentProperty, Object, Object> {

	/**
	 * Creates table specification for a given entity
	 * 
	 * @param entity
	 * @return CreateTableSpecification for this entity
	 */

	CreateTableSpecification getCreateTableSpecification(CassandraPersistentEntity<?> entity);

	/**
	 * Checks existing table in Cassandra with entity information. Creates alter table specification if has differences.
	 * 
	 * @param entity
	 * @param table
	 * @return AlterTableSpecification or null
	 */

	AlterTableSpecification getAlterTableSpecification(CassandraPersistentEntity<?> entity, TableMetadata table,
			boolean dropRemovedAttributeColumns);

	/**
	 * Get all create index specifications for the entity
	 * 
	 * @param entity
	 * @return list of all CreateIndexSpecifications in the indexes
	 */

	List<CreateIndexSpecification> getCreateIndexSpecifications(CassandraPersistentEntity<?> entity);

	/**
	 * Get index change specifications for the entity
	 * 
	 * @param entity
	 * @return list of all CreateIndexSpecifications in the indexes
	 */

	List<WithNameSpecification<?>> getIndexChangeSpecifications(CassandraPersistentEntity<?> entity, TableMetadata table);

	/**
	 * Get the primary key from entity
	 * 
	 * @param entity
	 * @param id
	 * @return
	 */
	List<Clause> getPrimaryKey(CassandraPersistentEntity<?> entity, Object id);

    /**
     * Get the where clause for the partitioned part of the primary key for a persistent entity
     * @param entity persistent entity class
     * @param id persistent entity id. Must contain values for all partitioned columns. Clustering column values are
     *           ignored.
     * @return where clause
     */
    List<Clause> getPartitionKey(CassandraPersistentEntity<?> entity, Object id);
}
