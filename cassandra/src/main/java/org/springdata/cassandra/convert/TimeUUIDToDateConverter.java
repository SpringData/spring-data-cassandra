package org.springdata.cassandra.convert;

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
