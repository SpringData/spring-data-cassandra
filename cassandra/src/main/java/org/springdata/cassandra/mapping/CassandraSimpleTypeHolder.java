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
package org.springdata.cassandra.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

import com.datastax.driver.core.DataType;

/**
 * Simple constant holder for a {@link SimpleTypeHolder} enriched with Cassandra specific simple types.
 * 
 * @author Alex Shvid
 */
public class CassandraSimpleTypeHolder extends SimpleTypeHolder {

	private static final Map<Class<?>, Class<?>> wrapperToPrimitiveTypeMap = new HashMap<Class<?>, Class<?>>(8);

	private static final Map<Class<?>, DataType> javaClassToDataTypeMap = new HashMap<Class<?>, DataType>();

	private static final Map<DataType.Name, DataType> nameToDataTypeMap = new HashMap<DataType.Name, DataType>();

	private static final Set<Class<?>> CASSANDRA_SIMPLE_TYPES;

	static {

		wrapperToPrimitiveTypeMap.put(Boolean.class, boolean.class);
		wrapperToPrimitiveTypeMap.put(Byte.class, byte.class);
		wrapperToPrimitiveTypeMap.put(Character.class, char.class);
		wrapperToPrimitiveTypeMap.put(Double.class, double.class);
		wrapperToPrimitiveTypeMap.put(Float.class, float.class);
		wrapperToPrimitiveTypeMap.put(Integer.class, int.class);
		wrapperToPrimitiveTypeMap.put(Long.class, long.class);
		wrapperToPrimitiveTypeMap.put(Short.class, short.class);

		Set<Class<?>> simpleTypes = new HashSet<Class<?>>();

		for (DataType dataType : DataType.allPrimitiveTypes()) {

			Class<?> javaClass = dataType.asJavaClass();
			simpleTypes.add(javaClass);

			javaClassToDataTypeMap.put(javaClass, dataType);

			Class<?> primitiveJavaClass = wrapperToPrimitiveTypeMap.get(javaClass);
			if (primitiveJavaClass != null) {
				javaClassToDataTypeMap.put(primitiveJavaClass, dataType);
				simpleTypes.add(primitiveJavaClass);
			}

			nameToDataTypeMap.put(dataType.getName(), dataType);
		}

		javaClassToDataTypeMap.put(String.class, DataType.text());
		javaClassToDataTypeMap.put(Enum.class, DataType.ascii());

		CASSANDRA_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);

	}

	public CassandraSimpleTypeHolder() {
		super(CASSANDRA_SIMPLE_TYPES, false);
	}

	public static DataType getDataTypeByName(DataType.Name name) {
		return nameToDataTypeMap.get(name);
	}

	public static DataType getDataTypeByJavaClass(Class<?> javaClass) {
		return javaClassToDataTypeMap.get(javaClass);
	}

	public static DataType.Name[] getDataTypeNamesForArguments(List<TypeInformation<?>> arguments) {

		DataType.Name[] result = new DataType.Name[arguments.size()];

		for (int i = 0; i != result.length; ++i) {

			TypeInformation<?> type = arguments.get(i);

			Class<?> javaClass = type.getType();

			DataType dataType = getDataTypeByJavaClass(javaClass);

			if (dataType == null) {
				throw new InvalidDataAccessApiUsageException("not found appropriate DataType for javaClass=" + javaClass);
			}

			result[i] = dataType.getName();
		}

		return result;
	}

}
