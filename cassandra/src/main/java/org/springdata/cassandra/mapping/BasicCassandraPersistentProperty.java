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
package org.springdata.cassandra.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.springdata.cassandra.mapping.support.CamelCaseToUnderscoreConverter;
import org.springdata.cassandra.mapping.support.DateToTimeUUIDConverter;
import org.springdata.cassandra.mapping.support.EnumToStringConverter;
import org.springdata.cassandra.mapping.support.StringToEnumConverter;
import org.springdata.cassandra.mapping.support.TimeUUIDToDateConverter;
import org.springdata.cql.core.KeyPart;
import org.springdata.cql.core.Ordering;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;

/**
 * Cassandra specific {@link org.springframework.data.mapping.model.AnnotationBasedPersistentProperty} implementation.
 * 
 * @author Alex Shvid
 */
public class BasicCassandraPersistentProperty extends AnnotationBasedPersistentProperty<CassandraPersistentProperty>
		implements CassandraPersistentProperty {

	private final Converter<String, String> fieldNameToColumnNameConverter;

	/**
	 * Creates a new {@link BasicCassandraPersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 */
	public BasicCassandraPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			CassandraPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);
		fieldNameToColumnNameConverter = CamelCaseToUnderscoreConverter.INSTANCE;
	}

	/**
	 * Also considers fields that has an Id annotation.
	 * 
	 */
	@Override
	public boolean isIdProperty() {

		if (super.isIdProperty()) {
			return true;
		}

		return isAnnotationPresent(Id.class);
	}

	/**
	 * Returns the true if the field composite primary key.
	 * 
	 * @return
	 */
	@Override
	public boolean hasEmbeddableType() {
		Class<?> fieldType = getField().getType();
		return fieldType.isAnnotationPresent(Embeddable.class);
	}

	/**
	 * Returns the column name to be used to store the value of the property inside the Cassandra.
	 * 
	 * @return
	 */
	public String getColumnName() {

		Column column = findAnnotation(Column.class);
		if (column != null && StringUtils.hasText(column.value())) {
			return column.value();
		}

		return fieldNameToColumnNameConverter.convert(field.getName());
	}

	/**
	 * Returns ordering for the column. Valid only for clustered columns.
	 * 
	 * @return
	 */
	public Ordering getOrdering() {
		PrimaryKey annotation = findAnnotation(PrimaryKey.class);
		return annotation != null ? annotation.ordering() : null;
	}

	/**
	 * Returns the data type information if exists.
	 * 
	 * @return
	 */
	public DataType getDataType() {

		Class<?> propertyType = getType();

		Qualify annotation = findAnnotation(Qualify.class);
		if (annotation != null && annotation.type() != null) {
			return qualifyAnnotatedType(annotation);
		}

		if (Enum.class.isAssignableFrom(propertyType)) {
			return DataType.text();
		}

		if (isMap()) {
			List<TypeInformation<?>> args = getTypeInformation().getTypeArguments();
			ensureTypeArguments(args.size(), 2);

			return DataType.map(autodetectPrimitiveType(args.get(0).getType()),
					autodetectPrimitiveType(args.get(1).getType()));
		}

		if (isCollectionLike()) {
			List<TypeInformation<?>> args = getTypeInformation().getTypeArguments();
			ensureTypeArguments(args.size(), 1);

			if (Set.class.isAssignableFrom(propertyType)) {

				return DataType.set(autodetectPrimitiveType(args.get(0).getType()));

			} else if (List.class.isAssignableFrom(propertyType)) {

				return DataType.list(autodetectPrimitiveType(args.get(0).getType()));

			}
		}

		DataType dataType = CassandraSimpleTypeHolder.getDataTypeByJavaClass(propertyType);
		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(
					"only primitive types and Set,List,Map collections are allowed, unknown type for property '" + this.getName()
							+ "' type is '" + propertyType + "' in the entity " + this.getOwner().getName());
		}

		return dataType;
	}

	private DataType qualifyAnnotatedType(Qualify annotation) {
		DataType.Name type = annotation.type();
		if (type.isCollection()) {
			switch (type) {
			case MAP:
				ensureTypeArguments(annotation.typeArguments().length, 2);
				return DataType.map(resolvePrimitiveType(annotation.typeArguments()[0]),
						resolvePrimitiveType(annotation.typeArguments()[1]));
			case LIST:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.list(resolvePrimitiveType(annotation.typeArguments()[0]));
			case SET:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.set(resolvePrimitiveType(annotation.typeArguments()[0]));
			default:
				throw new InvalidDataAccessApiUsageException("unknown collection DataType for property '" + this.getName()
						+ "' type is '" + this.getType() + "' in the entity " + this.getOwner().getName());
			}
		} else {
			return CassandraSimpleTypeHolder.getDataTypeByName(type);
		}
	}

	/**
	 * Returns true if the property has secondary index on this column.
	 * 
	 * @return
	 */
	public boolean isIndexed() {
		return isAnnotationPresent(Indexed.class);
	}

	/**
	 * Returns index name for the column.
	 * 
	 * @return
	 */
	public String getIndexName() {
		Indexed indexed = findAnnotation(Indexed.class);
		if (indexed != null) {
			return StringUtils.hasText(indexed.name()) ? indexed.name() : null;
		}
		return null;
	}

	/**
	 * Returns true if the property has PartitionKey annotation on this column.
	 * 
	 * @return
	 */
	public KeyPart getKeyPart() {
		PrimaryKey keyColumn = findAnnotation(PrimaryKey.class);
		if (keyColumn != null) {
			return keyColumn.keyPart();
		}
		return null;
	}

	/**
	 * Returns true if the property has Clustered annotation on this column.
	 * 
	 * @return
	 */
	public Integer getOrdinal() {
		PrimaryKey keyColumn = findAnnotation(PrimaryKey.class);
		if (keyColumn != null) {
			return keyColumn.ordinal();
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<CassandraPersistentProperty> createAssociation() {
		return new Association<CassandraPersistentProperty>(this, null);
	}

	DataType resolvePrimitiveType(DataType.Name typeName) {
		DataType dataType = CassandraSimpleTypeHolder.getDataTypeByName(typeName);
		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(
					"only primitive types are allowed inside collections for the property  '" + this.getName() + "' type is '"
							+ this.getType() + "' in the entity " + this.getOwner().getName());
		}
		return dataType;
	}

	DataType autodetectPrimitiveType(Class<?> javaType) {
		DataType dataType = CassandraSimpleTypeHolder.getDataTypeByJavaClass(javaType);
		if (dataType == null) {
			throw new InvalidDataAccessApiUsageException(
					"only primitive types are allowed inside collections for the property  '" + this.getName() + "' type is '"
							+ this.getType() + "' in the entity " + this.getOwner().getName());
		}
		return dataType;
	}

	void ensureTypeArguments(int args, int expected) {
		if (args != expected) {
			throw new InvalidDataAccessApiUsageException("expected " + expected + " of typed arguments for the property  '"
					+ this.getName() + "' type is '" + this.getType() + "' in the entity " + this.getOwner().getName());
		}
	}

	@Override
	public Converter<?, ?> getReadConverter() {

		Class<?> propertyType = getType();

		if (Enum.class.isAssignableFrom(propertyType)) {
			return new StringToEnumConverter(propertyType);
		}

		DataType dataType = getDataType();

		if (dataType.getName() == DataType.Name.TIMEUUID && propertyType == Date.class) {
			return TimeUUIDToDateConverter.INSTANCE;
		}

		return null;
	}

	@Override
	public Converter<?, ?> getWriteConverter() {

		Class<?> propertyType = getType();

		if (Enum.class.isAssignableFrom(propertyType)) {
			return EnumToStringConverter.INSTANCE;
		}

		DataType dataType = getDataType();

		if (dataType.getName() == DataType.Name.TIMEUUID && propertyType == Date.class) {
			return DateToTimeUUIDConverter.INSTANCE;
		}
		return null;
	}

}
