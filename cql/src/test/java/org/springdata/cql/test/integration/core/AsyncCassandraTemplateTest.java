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

import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import org.junit.Test;
import org.springdata.cql.core.SimpleQueryCreator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * @author David Webb
 * @author Alex Shvid
 */
public class AsyncCassandraTemplateTest extends AbstractCassandraOperations {

	@Test
	public void executeAsynchronouslyTestCqlString() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cqlTemplate.buildExecuteOperation(
				new SimpleQueryCreator("insert into book (isbn, title, author, pages) values ('" + isbn + "', '" + title
						+ "', '" + author + "', " + pages + ")")).executeAsync();

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void queryAsynchronouslyTestCqlStringResultSetCallback() {

		final String isbn = "999999999";

		ResultSet frs = cqlTemplate.buildSelectOperation("select * from book where isbn='" + isbn + "'").executeAsync()
				.getUninterruptibly();

		Row r = frs.one();
		assertNotNull(r);

		Book b1 = rowToBook(r);

		Book b2 = getBook(isbn);

		assertBook(b1, b2);

	}

}
