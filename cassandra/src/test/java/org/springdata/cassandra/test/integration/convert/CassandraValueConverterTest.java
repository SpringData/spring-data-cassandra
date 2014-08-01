package org.springdata.cassandra.test.integration.convert;

import org.junit.Assert;
import org.junit.Test;

public class CassandraValueConverterTest {

	@Test
	public void testEnum() {

		Class<? extends Enum> enumClass = EventType.class;

		EventType type = (EventType) Enum.valueOf(enumClass, "LOGIN");

		Assert.assertEquals(EventType.LOGIN, type);

	}

	enum EventType {
		LOGIN;
	}

}
