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

import org.springdata.cassandra.convert.CassandraConverter;
import org.springdata.cassandra.core.CassandraOperations;
import org.springdata.cassandra.core.CassandraTemplate;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import com.datastax.driver.core.Session;

/**
 * Convenient factory for configuring a CassandraTemplate. It is enough to have one CassandraTemplate per application.
 * 
 * @author Alex Shvid
 */

public class CassandraTemplateFactoryBean implements FactoryBean<CassandraTemplate>, InitializingBean {

	private CassandraTemplate cassandraTemplate;

	private Session session;
	private String keyspace;
	private CassandraConverter converter;

	@Override
	public CassandraTemplate getObject() {
		return cassandraTemplate;
	}

	@Override
	public Class<? extends CassandraOperations> getObjectType() {
		return CassandraTemplate.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() {

		if (session == null) {
			throw new IllegalArgumentException("session is required");
		}

		if (converter == null) {
			throw new IllegalArgumentException("converter is required");
		}

		// initialize property
		this.cassandraTemplate = new CassandraTemplate(session, converter, keyspace);

	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public void setConverter(CassandraConverter converter) {
		this.converter = converter;
	}

}
