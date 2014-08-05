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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.springdata.cql.core.PreparedStatementBinder;
import org.springdata.cql.core.PreparedStatementCallback;
import org.springdata.cql.core.PreparedStatementCreator;
import org.springdata.cql.core.ResultSetExtractor;
import org.springdata.cql.core.RowCallbackHandler;
import org.springdata.cql.core.RowMapper;
import org.springdata.cql.core.SimplePreparedStatementQueryCreator;
import org.springdata.cql.core.SimpleQueryCreator;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author David Webb
 * @author Alex Shvid
 */
public class PreparedStatementTest extends AbstractCassandraOperations {

	@Test
	public void executeTestCqlStringPreparedStatementCallback() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		PreparedStatement ps = cqlTemplate.prepareStatement(cql);

		BoundStatement statement = cqlTemplate.execute(ps, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doWithPreparedStatement(Session session, PreparedStatement ps) {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void executeTestPreparedStatementCreatorPreparedStatementCallback() {

		final String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		PreparedStatement ps = cqlTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement statement = cqlTemplate.execute(ps, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doWithPreparedStatement(Session session, PreparedStatement ps) {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderResultSetCallback() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cqlTemplate.prepareStatement(cql);

		BoundStatement bs = cqlTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		Book b1 = cqlTemplate.getSelectOperation(bs).transform(new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) {
				Row r = rs.one();
				assertNotNull(r);

				Book b = rowToBook(r);

				return b;
			}
		}).execute();

		Book b2 = getBook(isbn);

		assertBook(b1, b2);
	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderRowCallbackHandler() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cqlTemplate.prepareStatement(cql);

		cqlTemplate.getSelectOperation(new SimplePreparedStatementQueryCreator(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		})).forEach(new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				Book b = rowToBook(row);

				Book b2 = getBook(isbn);

				assertBook(b, b2);

			}
		}).execute();

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderRowMapper() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cqlTemplate.prepareStatement(cql);

		List<Book> books = cqlTemplate.getSelectOperation(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}).map(new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		}).execute();

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorResultSetCallback() {

		insertBooks();

		final String cql = "select * from book";

		PreparedStatement ps = cqlTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cqlTemplate.bind(ps);

		List<Book> books = cqlTemplate.getSelectOperation(bs).transform(new ResultSetExtractor<List<Book>>() {

			@Override
			public List<Book> extractData(ResultSet rs) {

				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		}).execute();

		log.debug("Size of all Books -> " + books.size());

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorRowCallbackHandler() {

		insertBooks();

		final String cql = "select * from book";

		PreparedStatement ps = cqlTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cqlTemplate.bind(ps);

		cqlTemplate.getSelectOperation(bs).forEach(new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				Book b = rowToBook(row);

				log.debug("Title -> " + b.getTitle());

			}
		}).execute();

	}

	@Test
	public void queryTestPreparedStatementCreatorRowMapper() {

		insertBooks();

		final String cql = "select * from book";

		PreparedStatement ps = cqlTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cqlTemplate.bind(ps);

		List<Book> books = cqlTemplate.getSelectOperation(bs).map(new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		}).execute();

		log.debug("Size of all Books -> " + books.size());

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderResultSetCallback() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cqlTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cqlTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		List<Book> books = cqlTemplate.getSelectOperation(new SimpleQueryCreator(bs)).transform(new ResultSetExtractor<List<Book>>() {

			@Override
			public List<Book> extractData(ResultSet rs) {
				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		}).execute();

		Book b2 = getBook(isbn);

		log.debug("Book list Size -> " + books.size());

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderRowCallbackHandler() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cqlTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cqlTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		cqlTemplate.getSelectOperation(new SimpleQueryCreator(bs)).forEach(new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {
				Book b = rowToBook(row);
				Book b2 = getBook(isbn);
				assertBook(b, b2);
			}
		}).execute();

	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderRowMapper() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cqlTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cqlTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		List<Book> books = cqlTemplate.getSelectOperation(new SimpleQueryCreator(bs)).map(new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		}).execute();

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

}
