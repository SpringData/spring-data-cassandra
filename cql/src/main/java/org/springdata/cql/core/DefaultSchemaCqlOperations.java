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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cql.generator.AlterTableCqlGenerator;
import org.springdata.cql.generator.CreateIndexCqlGenerator;
import org.springdata.cql.generator.CreateTableCqlGenerator;
import org.springdata.cql.generator.DropIndexCqlGenerator;
import org.springdata.cql.generator.DropTableCqlGenerator;
import org.springdata.cql.option.TableOptions;
import org.springdata.cql.spec.AlterTableSpecification;
import org.springdata.cql.spec.CreateIndexSpecification;
import org.springdata.cql.spec.CreateTableSpecification;
import org.springdata.cql.spec.DropIndexSpecification;
import org.springdata.cql.spec.DropTableSpecification;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Default Table Operations implementation
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultSchemaCqlOperations implements SchemaCqlOperations {

	private static final Logger log = LoggerFactory.getLogger(DefaultSchemaCqlOperations.class);

	private final CqlTemplate cqlTemplate;
	private final String keyspace;

	protected DefaultSchemaCqlOperations(CqlTemplate cqlTemplate, String keyspace) {

		Assert.notNull(cqlTemplate);
		Assert.notNull(keyspace);

		this.cqlTemplate = cqlTemplate;
		this.keyspace = keyspace;
	}

	@Override
	public TableMetadata getTableMetadata(final String tableName) {

		return cqlTemplate.doExecute(new SessionCallback<TableMetadata>() {

			public TableMetadata doInSession(Session s) {

				log.debug("getTableMetadata keyspace => " + keyspace + ", table => " + tableName);

				return s.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase()).getTable(tableName.toLowerCase());
			}
		});
	}

	@Override
	public UpdateOperation createTable(boolean ifNotExists, String tableName, List<CqlColumn> partitionedColumns,
			List<ClusteringCqlColumn> clusteringColumns, List<CqlColumn> nonKeyColumns, TableOptions tableOptions) {
		Assert.notNull(tableName);
		Assert.notNull(partitionedColumns);
		Assert.notNull(clusteringColumns);
		Assert.notNull(nonKeyColumns);
		Assert.notNull(tableOptions);

		CreateTableSpecification spec = new CreateTableSpecification().ifNotExists(ifNotExists).name(tableName);
		spec.with(tableOptions.getOptions());

		for (CqlColumn cqlColumn : partitionedColumns) {
			spec.partitionKeyColumn(cqlColumn.getName(), cqlColumn.getType());
		}

		for (ClusteringCqlColumn cqlColumn : clusteringColumns) {
			spec.clusteringKeyColumn(cqlColumn.getName(), cqlColumn.getType(), cqlColumn.getOrdering());
		}

		for (CqlColumn cqlColumn : nonKeyColumns) {
			spec.column(cqlColumn.getName(), cqlColumn.getType());
		}
		return createTable(spec);
	}

	@Override
	public UpdateOperation createTable(CreateTableSpecification createTableSpecification) {
		Assert.notNull(createTableSpecification);

		CreateTableCqlGenerator generator = new CreateTableCqlGenerator(createTableSpecification);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());
	}

	@Override
	public UpdateOperation alterTable(String tableName, List<CqlColumn> addColumns, List<CqlColumn> alterColumns,
			List<String> dropColumns, TableOptions tableOptions) {
		Assert.notNull(tableName);
		Assert.notNull(addColumns);
		Assert.notNull(alterColumns);
		Assert.notNull(dropColumns);
		Assert.notNull(tableOptions);

		AlterTableSpecification spec = new AlterTableSpecification().name(tableName);
		spec.with(tableOptions.getOptions());

		for (CqlColumn cqlColumn : addColumns) {
			spec.add(cqlColumn.getName(), cqlColumn.getType());
		}

		for (CqlColumn cqlColumn : alterColumns) {
			spec.alter(cqlColumn.getName(), cqlColumn.getType());
		}

		for (String column : dropColumns) {
			spec.drop(column);
		}

		return alterTable(spec);

	}

	@Override
	public UpdateOperation alterTable(AlterTableSpecification alterTableSpecification) {
		Assert.notNull(alterTableSpecification);

		AlterTableCqlGenerator generator = new AlterTableCqlGenerator(alterTableSpecification);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());
	}

	@Override
	public UpdateOperation dropTable(boolean ifExists, String tableName) {
		Assert.notNull(tableName);

		DropTableSpecification spec = new DropTableSpecification().ifExists(ifExists).name(tableName);

		return dropTable(spec);
	}

	@Override
	public UpdateOperation dropTable(DropTableSpecification dropTableSpecification) {
		Assert.notNull(dropTableSpecification);

		DropTableCqlGenerator generator = new DropTableCqlGenerator(dropTableSpecification);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());
	}

	@Override
	public UpdateOperation createIndex(String indexName) {
		Assert.notNull(indexName);

		CreateIndexSpecification spec = new CreateIndexSpecification().name(indexName);

		return createIndex(spec);
	}

	@Override
	public UpdateOperation createIndex(String tableName, String columnName) {
		Assert.notNull(tableName);
		Assert.notNull(columnName);

		CreateIndexSpecification spec = new CreateIndexSpecification().on(tableName).column(columnName);

		return createIndex(spec);
	}

	@Override
	public UpdateOperation createIndex(CreateIndexSpecification createIndexSpecification) {
		Assert.notNull(createIndexSpecification);

		CreateIndexCqlGenerator generator = new CreateIndexCqlGenerator(createIndexSpecification);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());
	}

	@Override
	public UpdateOperation dropIndex(String indexName) {
		Assert.notNull(indexName);

		DropIndexSpecification spec = new DropIndexSpecification().name(indexName);

		return dropIndex(spec);
	}

	@Override
	public UpdateOperation dropIndex(String tableName, String columnName) {
		Assert.notNull(tableName);
		Assert.notNull(columnName);

		DropIndexSpecification spec = new DropIndexSpecification().defaultName(tableName, columnName);

		return dropIndex(spec);
	}

	@Override
	public UpdateOperation dropIndex(DropIndexSpecification dropIndexSpecification) {
		Assert.notNull(dropIndexSpecification);

		DropIndexCqlGenerator generator = new DropIndexCqlGenerator(dropIndexSpecification);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());
	}

}
