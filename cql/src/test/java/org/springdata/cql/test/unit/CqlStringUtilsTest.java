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
package org.springdata.cql.test.unit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springdata.cql.util.CqlStringUtils.isQuotedIdentifier;
import static org.springdata.cql.util.CqlStringUtils.isUnquotedIdentifier;

import org.junit.Test;

public class CqlStringUtilsTest {

	@Test
	public void testIsQuotedIdentifier() throws Exception {
		assertFalse(isQuotedIdentifier("my\"id"));
		assertTrue(isQuotedIdentifier("my\"\"id"));
		assertFalse(isUnquotedIdentifier("my\"id"));
		assertTrue(isUnquotedIdentifier("myid"));
	}

}
