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
package org.springdata.cassandra.convert;

import java.security.SecureRandom;
import java.util.Date;
import java.util.UUID;

import org.springdata.cassandra.mapping.CassandraPersistentProperty;
import org.springdata.cql.support.UUIDBuilder;

import com.datastax.driver.core.DataType;

/**
 * Simple converter for values
 * 
 * @author Alex Shvid
 * 
 */

public final class CassandraValueConverter {

	/*
	 * The random number generator used by this class to create random
	 * clockSequence and node in UUIDs.
	 */
	private static class Holder {
		static final SecureRandom numberGenerator = new SecureRandom();
	}

	private CassandraValueConverter() {
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Object afterRead(CassandraPersistentProperty property, Object value) {

		if (value != null) {

			if (property.isEnum()) {
				return Enum.valueOf((Class<? extends Enum>) property.getType(), value.toString());
			}

			DataType dataType = property.getDataType();

			if (dataType.getName() == DataType.Name.TIMEUUID && value instanceof UUID) {

				UUID uuid = (UUID) value;

				if (property.getType() == Date.class) {
					return new Date(UUIDBuilder.getTimestampMillis(uuid));
				} else if (property.getType() == java.sql.Date.class) {
					return new java.sql.Date(UUIDBuilder.getTimestampMillis(uuid));
				}

				return uuid;

			}

		}

		return value;
	}

	public static Object beforeWrite(CassandraPersistentProperty property, Object value) {

		if (value == null) {
			return value;
		}

		if (value.getClass().isEnum()) {
			return ((Enum) value).name();
		}

		DataType dataType = property.getDataType();

		if (dataType.getName() == DataType.Name.TIMEUUID && value instanceof Date) {

			long milliseconds = ((Date) value).getTime();

			UUID uuid = new UUIDBuilder().addVersion(1).addTimestampMillis(milliseconds)
					.addClockSequence(randomClockSequence()).addNode(randomNode()).build();

			return uuid;

		}

		return value;
	}

	private static int randomClockSequence() {
		return Holder.numberGenerator.nextInt(0x3fff);
	}

	private static long randomNode() {
		return Holder.numberGenerator.nextLong() & 0xFFFFFFFFFFFFL;
	}
}
