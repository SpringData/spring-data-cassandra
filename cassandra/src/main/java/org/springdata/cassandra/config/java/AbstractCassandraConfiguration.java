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
package org.springdata.cassandra.config.java;

import java.util.HashSet;
import java.util.Set;

import org.springdata.cassandra.convert.CassandraConverter;
import org.springdata.cassandra.convert.MappingCassandraConverter;
import org.springdata.cassandra.core.CassandraSessionFactoryBean;
import org.springdata.cassandra.core.CassandraTemplate;
import org.springdata.cassandra.core.CassandraTemplateFactoryBean;
import org.springdata.cassandra.mapping.CassandraMappingContext;
import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.mapping.CassandraPersistentProperty;
import org.springdata.cassandra.mapping.Table;
import org.springdata.cql.config.KeyspaceAttributes;
import org.springdata.cql.config.java.AbstractCassandraClusterConfiguration;
import org.springdata.cql.core.CqlTemplate;
import org.springdata.cql.core.CqlTemplateFactoryBean;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Session;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 */
@Configuration
public abstract class AbstractCassandraConfiguration extends AbstractCassandraClusterConfiguration implements
		BeanClassLoaderAware {

	/**
	 * Used by CassandraConverter
	 */
	private ClassLoader beanClassLoader;

	/**
	 * Return the name of the keyspace to connect to.
	 * 
	 * @return for {@literal null} or empty keyspace will be used SYSTEM keyspace by default.
	 */
	protected abstract String keyspace();

	/**
	 * Return keyspace attributes
	 * 
	 * @return KeyspaceAttributes
	 */
	@Bean
	public KeyspaceAttributes keyspaceAttributes() {
		return new KeyspaceAttributes();
	}

	/**
	 * Creates a {@link Session}. Will create, verify or drop tables in Cassandra on creation/destroy stage.
	 * 
	 * @see #cluster()
	 * @see #Keyspace()
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandraSessionFactoryBean session() throws Exception {
		CassandraSessionFactoryBean factory = new CassandraSessionFactoryBean();
		factory.setKeyspace(keyspace());
		factory.setCluster(cluster().getObject());
		factory.setConverter(converter());
		factory.setKeyspaceAttributes(keyspaceAttributes());
		factory.setBeanClassLoader(beanClassLoader);
		return factory;
	}

	/**
	 * Return the base package to scan for mapped {@link Table}s. Will return the package name of the configuration class'
	 * (the concrete class, not this one here) by default. So if you have a {@code com.acme.AppConfig} extending
	 * {@link AbstractCassandraConfiguration} the base package will be considered {@code com.acme} unless the method is
	 * overriden to implement alternate behaviour.
	 * 
	 * @return the base package to scan for mapped {@link Table} classes or {@literal null} to not enable scanning for
	 *         entities.
	 */
	protected String getMappingBasePackage() {
		return getClass().getPackage().getName();
	}

	/**
	 * Creates a {@link CqlTemplate}.
	 * 
	 * @return CqlOperations
	 */
	@Bean
	public CqlTemplateFactoryBean cqlTemplate() throws Exception {
		CqlTemplateFactoryBean factory = new CqlTemplateFactoryBean();
		factory.setKeyspace(keyspace());
		factory.setSession(session().getObject());
		return factory;
	}

	/**
	 * Creates a {@link CassandraTemplate}.
	 * 
	 * @return CassandraOperations
	 */
	@Bean
	public CassandraTemplateFactoryBean template() throws Exception {
		CassandraTemplateFactoryBean factory = new CassandraTemplateFactoryBean();
		factory.setKeyspace(keyspace());
		factory.setSession(session().getObject());
		factory.setConverter(converter());
		return factory;
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext() {
		CassandraMappingContext context = new CassandraMappingContext();
		context.setInitialEntitySet(getInitialEntitySet());
		return context;
	}

	/**
	 * Return the {@link CassandraConverter} instance to convert Rows to Objects, Objects to BuiltStatements
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandraConverter converter() {
		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext());
		converter.setBeanClassLoader(beanClassLoader);
		return converter;
	}

	/**
	 * Scans the mapping base package for classes annotated with {@link Table}.
	 * 
	 * @see #getMappingBasePackage()
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected Set<Class<?>> getInitialEntitySet() {

		String basePackage = getMappingBasePackage();
		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Table.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(loadClass(candidate.getBeanClassName()));
			}
		}

		return initialEntitySet;
	}

	/**
	 * Bean ClassLoader Aware for CassandraTemplate/CassandraAdminTemplate
	 */
	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	/**
	 * Service method to load class and transform ClassNotFoundException to IllegalArgumentException
	 * 
	 * @param className
	 * @return
	 */

	private Class<?> loadClass(String className) {
		try {
			return ClassUtils.forName(className, this.beanClassLoader);
		} catch (Exception e) {
			throw new IllegalArgumentException("class not found " + className, e);
		}
	}

}
