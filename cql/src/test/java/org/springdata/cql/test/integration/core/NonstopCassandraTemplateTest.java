/*
 * Copyright 2014 the original author or authors.
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

import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.springdata.cql.core.ResultSetExtractor;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * @author Alex Shvid
 */
public class NonstopCassandraTemplateTest extends AbstractCassandraOperations {

	@Test
	public void queryTestCqlStringResultSetCallbackNonstop() throws TimeoutException {

		final String isbn = "999999999";

		Book b1 = cqlTemplate.getSelectOperation("select * from book where isbn='" + isbn + "'")
				.transform(new ResultSetExtractor<Book>() {

					@Override
					public Book extractData(ResultSet rs) {
						Row r = rs.one();
						assertNotNull(r);

						Book b = rowToBook(r);

						return b;
					}
				}).executeNonstop(10000);

		Book b2 = getBook(isbn);

		assertBook(b1, b2);

	}

}
