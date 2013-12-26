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
package org.springdata.cassandra.data.test.integration.config;

import org.springdata.cassandra.data.config.AbstractCassandraConfiguration;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

/**
 * Simple JavaConfig
 * 
 * @author Alex Shvid
 * 
 */
@Configuration
public class JavaConfig extends AbstractCassandraConfiguration {

	@Override
	protected String keyspace() {
		return "test";
	}

	@Override
	public Cluster cluster() {
		Builder builder = Cluster.builder();
		builder.addContactPoint("localhost").withPort(9042);
		return builder.build();
	}
}