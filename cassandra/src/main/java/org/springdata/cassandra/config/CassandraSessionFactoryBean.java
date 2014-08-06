/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springdata.cassandra.config;

import java.util.Collection;
import java.util.List;

import org.springdata.cassandra.convert.CassandraConverter;
import org.springdata.cassandra.core.CassandraTemplate;
import org.springdata.cql.config.CqlSessionFactoryBean;
import org.springdata.cql.core.CqlTemplate;
import org.springdata.cql.core.ExecuteOperation;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Optional;

/**
 * Convenient factory for configuring a Cassandra Session.
 * 
 * @author Alex Shvid
 */

public class CassandraSessionFactoryBean extends CqlSessionFactoryBean implements FactoryBean<Session>, InitializingBean,
		DisposableBean, BeanClassLoaderAware, PersistenceExceptionTranslator {

	private ClassLoader beanClassLoader;

	private CassandraConverter converter;

	private Collection<TableAttributes> tables;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	@Override
	public void afterPropertiesSet() {

		if (converter == null) {
			throw new IllegalArgumentException("converter is required");
		}

		super.afterPropertiesSet();

		if (StringUtils.hasText(keyspace)) {

			CqlTemplate cqlTemplate = new CqlTemplate(session, keyspace);
			CassandraTemplate cassandraTemplate = new CassandraTemplate(session, converter, keyspace);

			if (!CollectionUtils.isEmpty(tables)) {

				for (TableAttributes tableAttributes : tables) {

					String entityClassName = tableAttributes.getEntityClass();
					Class<?> entityClass = loadClass(entityClassName);

					String useTableName = tableAttributes.getTableName() != null ? tableAttributes.getTableName()
							: cassandraTemplate.getTableName(entityClass);

					if (keyspaceCreated) {
						createNewTable(cassandraTemplate, useTableName, entityClass);
					} else if (keyspaceAttributes.isUpdate()) {
						TableMetadata table = cqlTemplate.getSchemaOperations().getTableMetadata(useTableName);
						if (table == null) {
							createNewTable(cassandraTemplate, useTableName, entityClass);
						} else {

							Optional<ExecuteOperation> alter = cassandraTemplate.getSchemaOperations().alterTable(useTableName, entityClass,
									true);
							if (alter.isPresent()) {
								alter.get().execute();
							}

							cassandraTemplate.getSchemaOperations().alterIndexes(useTableName, entityClass).execute();

						}
					} else if (keyspaceAttributes.isValidate()) {

						TableMetadata table = cqlTemplate.getSchemaOperations().getTableMetadata(useTableName);
						if (table == null) {
							throw new InvalidDataAccessApiUsageException("not found table " + useTableName + " for entity "
									+ entityClassName);
						}

						String query = cassandraTemplate.getSchemaOperations().validateTable(useTableName, entityClass);

						if (query != null) {
							throw new InvalidDataAccessApiUsageException("invalid table " + useTableName + " for entity "
									+ entityClassName + ". modify it by " + query);
						}

						List<String> queryList = cassandraTemplate.getSchemaOperations().validateIndexes(useTableName, entityClass);

						if (!queryList.isEmpty()) {
							throw new InvalidDataAccessApiUsageException("invalid indexes in table " + useTableName + " for entity "
									+ entityClassName + ". modify it by " + queryList);
						}

					}

				}
			}
		}

	}

	private Class<?> loadClass(String className) {
		try {
			return ClassUtils.forName(className, this.beanClassLoader);
		} catch (Exception e) {
			throw new IllegalArgumentException("class not found " + className, e);
		}
	}

	private void createNewTable(CassandraTemplate cassandraTemplate, String useTableName, Class<?> entityClass) {
		cassandraTemplate.getSchemaOperations().createTable(useTableName, entityClass).execute();
		cassandraTemplate.getSchemaOperations().createIndexes(useTableName, entityClass).execute();
	}

	public void setConverter(CassandraConverter converter) {
		this.converter = converter;
	}

	public void setTables(Collection<TableAttributes> tables) {
		this.tables = tables;
	}

}
