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
package org.springdata.cql.config;

import org.springdata.cql.core.CqlOperations;
import org.springdata.cql.core.CqlTemplate;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import com.datastax.driver.core.Session;

/**
 * Convenient factory for configuring a Cassandra Driver Template. It is enough to have one session per application.
 * 
 * @author Alex Shvid
 */

public class CqlTemplateFactoryBean implements FactoryBean<CqlTemplate>, InitializingBean {

	private CqlTemplate cqlTemplate;

	private Session session;
	private String keyspace;

	@Override
	public CqlTemplate getObject() {
		return cqlTemplate;
	}

	@Override
	public Class<? extends CqlOperations> getObjectType() {
		return CqlTemplate.class;
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

		// initialize property
		this.cqlTemplate = new CqlTemplate(session, keyspace);

	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setSession(Session session) {
		this.session = session;
	}

}
