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
package org.springdata.cql.test.integration.config;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Session;

/**
 * Simple JavaConfig Test
 * 
 * @author Alex Shvid
 * 
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = JavaCqlConfig.class)
public class JavaCqlConfigTest {

	@BeforeClass
	public static void startCassandra() throws ConfigurationException, TTransportException, IOException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

	@Inject
	Session session;

	@Test
	public void test() {
		Assert.assertNotNull(session);
	}
}
