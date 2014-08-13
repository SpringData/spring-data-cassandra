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
package org.springdata.cassandra.mapping.support;

import java.util.Date;
import java.util.UUID;

import org.springdata.cql.util.TimeUUIDUtil;
import org.springframework.core.convert.converter.Converter;

/**
 * Simple TimeUUID to Date Converter
 * 
 * @author Alex Shvid
 * 
 */

public enum TimeUUIDToDateConverter implements Converter<UUID, Date> {

	INSTANCE;

	@Override
	public Date convert(UUID source) {
		return new Date(TimeUUIDUtil.getTimestampMillis(source));
	}

}
