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
package org.springdata.cql.test.unit.util;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.springdata.cql.util.UUIDBuilder;

/**
 * UUID Builder jUnit Test
 * 
 * @author Alex Shvid
 * 
 */

public class UUIDBuilderTest {

	@Test
	public void testRandom() {

		UUID uuid = UUID.randomUUID();

		Assert.assertEquals(2, uuid.variant());
		Assert.assertEquals(4, uuid.version());
	}

	@Test
	public void testUnknown() {

		UUID uuid = new UUIDBuilder().build();

		Assert.assertEquals(2, uuid.variant());
		Assert.assertEquals(0, uuid.version());
	}

	@Test
	public void testZeroTimebased() {

		UUID uuid = new UUIDBuilder().addVersion(1).build();

		Assert.assertEquals(2, uuid.variant());
		Assert.assertEquals(1, uuid.version());
		Assert.assertEquals(0L, uuid.timestamp());
		Assert.assertEquals(0, uuid.clockSequence());
		Assert.assertEquals(0L, uuid.node());
	}

	@Test
	public void testCurrentTimestamp() {

		long timestamp = System.currentTimeMillis();

		UUID uuid = new UUIDBuilder().addVersion(1).addTimestampMillis(timestamp).build();

		Assert.assertEquals(2, uuid.variant());
		Assert.assertEquals(1, uuid.version());
		Assert.assertEquals(timestamp, UUIDBuilder.getTimestampMillis(uuid));
		Assert.assertEquals(0, uuid.clockSequence());
		Assert.assertEquals(0L, uuid.node());
	}

	@Test
	public void testMaxClockSequence() {

		int clockSequence = 0x3FFF;

		UUID uuid = new UUIDBuilder().addVersion(1).addClockSequence(clockSequence).build();

		Assert.assertEquals(2, uuid.variant());
		Assert.assertEquals(1, uuid.version());
		Assert.assertEquals(0L, uuid.timestamp());
		Assert.assertEquals(clockSequence, uuid.clockSequence());
		Assert.assertEquals(0L, uuid.node());
	}

	@Test
	public void testMaxNode() {

		long node = 0xffffffffffffL;

		UUID uuid = new UUIDBuilder().addVersion(1).addNode(node).build();

		Assert.assertEquals(2, uuid.variant());
		Assert.assertEquals(1, uuid.version());
		Assert.assertEquals(0L, uuid.timestamp());
		Assert.assertEquals(0, uuid.clockSequence());
		Assert.assertEquals(node, uuid.node());
	}
}
