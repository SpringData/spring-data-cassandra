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
package org.springdata.cassandra.cql.test.integration.core;

import java.util.Arrays;

import org.junit.Test;

/**
 * @author David Webb
 * @author Alex Shvid
 */
public class IngestionOprerationsTest extends AbstractCassandraOperations {

	@Test
	public void ingestionTestObjectArray() {

		insertBooks();

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));
	}

	@Test
	public void ingestionTestRowIterator() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		final Object[][] v = new Object[3][];
		v[0] = o1;
		v[1] = o2;
		v[2] = o3;

		cqlTemplate.ingest(cqlTemplate.prepareStatement(cql), Arrays.asList(v)).execute();

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));

	}

}
