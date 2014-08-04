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
package org.springdata.cassandra.cql.test.unit.generator;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springdata.cassandra.cql.generator.AlterTableCqlGenerator;
import org.springdata.cassandra.cql.spec.AlterTableSpecification;

import com.datastax.driver.core.DataType;

public class AlterTableCqlGeneratorTests {

	/**
	 * Asserts that the preamble is first & correctly formatted in the given CQL string.
	 */
	public static void assertPreamble(String tableName, String cql) {
		assertTrue(cql.startsWith("ALTER TABLE " + tableName + " "));
	}

	/**
	 * Asserts that the given list of columns definitions are contained in the given CQL string properly.
	 * 
	 * @param columnSpec IE, "foo text, bar blob"
	 */
	public static void assertColumnChanges(String columnSpec, String cql) {
		assertTrue(cql.contains(""));
	}

	/**
	 * Convenient base class that other test classes can use so as not to repeat the generics declarations.
	 */
	public static abstract class AlterTableTest extends
			TableOperationCqlGeneratorTest<AlterTableSpecification, AlterTableCqlGenerator> {
	}

	public static class BasicTest extends AlterTableTest {

		public String name = "mytable";
		public DataType alteredType = DataType.text();
		public String altered = "altered";

		public DataType addedType = DataType.text();
		public String added = "added";

		public String dropped = "dropped";

		public AlterTableSpecification specification() {
			return new AlterTableSpecification().name(name).alter(altered, alteredType).add(added, addedType).drop(dropped);
		}

		public AlterTableCqlGenerator generator() {
			return new AlterTableCqlGenerator(specification);
		}

		@Test
		public void test() {
			prepare();

			assertPreamble(name, cql);
			assertColumnChanges(
					String.format("ALTER %s TYPE %s, ADD %s %s, DROP %s", altered, alteredType, added, addedType, dropped), cql);
		}
	}
}
