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
package org.springdata.cql.config.java;

import org.springdata.cql.config.CqlSessionFactoryBean;
import org.springdata.cql.config.CqlTemplateFactoryBean;
import org.springdata.cql.config.KeyspaceAttributes;
import org.springdata.cql.core.CqlTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 */
@Configuration
public abstract class AbstractCqlConfiguration extends AbstractClusterConfiguration {

	/**
	 * Return the name of the keyspace to connect to.
	 * 
	 * @return for {@literal null} or empty keyspace will be used SYSTEM keyspace by default.
	 */
	protected abstract String getKeyspace();

	/**
	 * Return keyspace attributes
	 * 
	 * @return KeyspaceAttributes
	 */
	public KeyspaceAttributes getKeyspaceAttributes() {
		return new KeyspaceAttributes();
	}

	/**
	 * Creates a {@link Session} to be used by the {@link CqlTemplate}. Will use the {@link Cluster} instance configured
	 * in {@link #cluster()}.
	 * 
	 * @see #cluster()
	 * @see #Keyspace()
	 * @return Session
	 */
	@Bean
	public CqlSessionFactoryBean session() throws Exception {
		CqlSessionFactoryBean factory = new CqlSessionFactoryBean();
		factory.setKeyspace(getKeyspace());
		factory.setCluster(cluster().getObject());
		factory.setKeyspaceAttributes(getKeyspaceAttributes());
		return factory;
	}

	/**
	 * Creates a {@link CqlTemplate}.
	 * 
	 * @return CqlOperations
	 */
	@Bean
	public CqlTemplateFactoryBean template() throws Exception {
		CqlTemplateFactoryBean factory = new CqlTemplateFactoryBean();
		factory.setKeyspace(getKeyspace());
		factory.setSession(session().getObject());
		return factory;
	}

}
