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
package org.springdata.cql.test.integration.core;

/**
 * @author David Webb
 * @author Alex Shvid
 */
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cql.core.CqlOperations;
import org.springdata.cql.core.CqlTemplate;
import org.springdata.cql.core.PreparedStatementBinder;
import org.springdata.cql.core.ResultSetExtractor;
import org.springdata.cql.test.integration.AbstractEmbeddedCassandraIntegrationTest;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public abstract class AbstractCassandraOperations extends AbstractEmbeddedCassandraIntegrationTest {

	protected CqlOperations cqlTemplate;

	protected Logger log = LoggerFactory.getLogger(getClass());

	/*
	 * Objects used for test data
	 */
	final String ISBN_NINES = "999999999";
	final String TITLE_NINES = "Book of Nines";
	final Object[] o1 = new Object[] { "1234", "Moby Dick", "Herman Manville", new Integer(456) };
	final Object[] o2 = new Object[] { "2345", "War and Peace", "Russian Dude", new Integer(456) };
	final Object[] o3 = new Object[] { "3456", "Jane Ayre", "Charlotte", new Integer(456) };

	/**
	 * This loads any test specific Cassandra objects
	 */
	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(
			"cassandraOperationsTest-cql-dataload.cql", this.keyspace), CASSANDRA_CONFIG, CASSANDRA_HOST,
			CASSANDRA_NATIVE_PORT);

	@Before
	public void setupTemplate() {
		cqlTemplate = new CqlTemplate(session, keyspace);
	}

	/**
	 * Assert that a Book matches the arguments expected
	 * 
	 * @param b
	 * @param orderedElements
	 */
	protected void assertBook(Book b, Object... orderedElements) {

		assertEquals(b.getIsbn(), orderedElements[0]);
		assertEquals(b.getTitle(), orderedElements[1]);
		assertEquals(b.getAuthor(), orderedElements[2]);
		assertEquals(b.getPages(), orderedElements[3]);

	}

	protected Book rowToBook(Row row) {
		Book b = new Book();
		b.setIsbn(row.getString("isbn"));
		b.setTitle(row.getString("title"));
		b.setAuthor(row.getString("author"));
		b.setPages(row.getInt("pages"));
		return b;
	}

	/**
	 * Convert Object[] to a Book
	 * 
	 * @param bookElements
	 * @return
	 */
	protected Book objectToBook(Object... bookElements) {
		Book b = new Book();
		b.setIsbn((String) bookElements[0]);
		b.setTitle((String) bookElements[1]);
		b.setAuthor((String) bookElements[2]);
		b.setPages((Integer) bookElements[3]);
		return b;
	}

	/**
	 * Convert List<Object> to a Book
	 * 
	 * @param bookElements
	 * @return
	 */
	protected Book listToBook(List<Object> bookElements) {
		Book b = new Book();
		b.setIsbn((String) bookElements.get(0));
		b.setTitle((String) bookElements.get(1));
		b.setAuthor((String) bookElements.get(2));
		b.setPages((Integer) bookElements.get(3));
		return b;

	}

	/**
	 * Assert that 2 Book objects are the same
	 * 
	 * @param b
	 * @param orderedElements
	 */
	protected void assertBook(Book b1, Book b2) {

		assertEquals(b1.getIsbn(), b2.getIsbn());
		assertEquals(b1.getTitle(), b2.getTitle());
		assertEquals(b1.getAuthor(), b2.getAuthor());
		assertEquals(b1.getPages(), b2.getPages());

	}

	/**
	 * Get a Book from Cassandra for assertions.
	 * 
	 * @param isbn
	 * @return
	 */
	protected Book getBook(final String isbn) {

		PreparedStatement ps = cqlTemplate.prepareStatement("select * from book where isbn = ?");

		Book b = cqlTemplate.getSelectOperation(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}).transform(

		new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) {
				Book b = new Book();
				Row r = rs.one();
				b.setIsbn(r.getString("isbn"));
				b.setTitle(r.getString("title"));
				b.setAuthor(r.getString("author"));
				b.setPages(r.getInt("pages"));
				return b;
			}
		}).execute();

		return b;

	}

	protected void insertBooks() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		Object[][] values = new Object[3][];
		values[0] = o1;
		values[1] = o2;
		values[2] = o3;

		cqlTemplate.getIngestOperation(cqlTemplate.prepareStatement(cql), values).execute();

	}

}
