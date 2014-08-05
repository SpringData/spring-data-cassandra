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
package org.springdata.cql.test.integration.generator;

import static org.springdata.cql.test.integration.generator.CqlTableSpecificationAssertions.assertTable;

import org.junit.Test;
import org.springdata.cql.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springdata.cql.test.unit.generator.CreateTableCqlGeneratorTests.BasicTest;
import org.springdata.cql.test.unit.generator.CreateTableCqlGeneratorTests.CompositePartitionKeyTest;
import org.springdata.cql.test.unit.generator.CreateTableCqlGeneratorTests.CreateTableTest;

/**
 * Integration tests that reuse unit tests.
 * 
 * @author Matthew T. Adams
 */
public class CreateTableCqlGeneratorIntegrationTests {

	/**
	 * Integration test base class that knows how to do everything except instantiate the concrete unit test type T.
	 * 
	 * @author Matthew T. Adams
	 * 
	 * @param <T> The concrete unit test class to which this integration test corresponds.
	 */
	public static abstract class Base<T extends CreateTableTest> extends AbstractEmbeddedCassandraIntegrationTest {
		T unit;

		public abstract T unit();

		@Test
		public void test() {
			unit = unit();
			unit.prepare();

			session.execute(unit.cql);

			assertTable(unit.specification, keyspace, session);
		}
	}

	public static class BasicIntegrationTest extends Base<BasicTest> {

		@Override
		public BasicTest unit() {
			return new BasicTest();
		}
	}

	public static class CompositePartitionKeyIntegrationTest extends Base<CompositePartitionKeyTest> {

		@Override
		public CompositePartitionKeyTest unit() {
			return new CompositePartitionKeyTest();
		}
	}
}