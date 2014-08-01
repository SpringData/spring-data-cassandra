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
package org.springdata.cassandra.test.integration.convert;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.hamcrest.number.BigDecimalCloseTo;
import org.hamcrest.number.IsCloseTo;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springdata.cassandra.core.CassandraOperations;
import org.springdata.cassandra.cql.core.CqlOperations;
import org.springdata.cassandra.test.integration.config.JavaConfig;
import org.springdata.cassandra.test.integration.table.BasicTypesEntity;
import org.springdata.cassandra.test.integration.table.CollectionTypesEntity;
import org.springdata.cassandra.test.integration.table.EmbeddedIdEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for data conversion related classes
 * 
 * @author Dzmitry Zhemchuhou
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { JavaConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraConversionTests {

	@Autowired
	private CqlOperations cqlOperations;

	@Autowired
	private CassandraOperations cassandraOperations;

	private final static String CASSANDRA_CONFIG = "cassandra.yaml";
	private final static String KEYSPACE_NAME = "test";
	private final static String CASSANDRA_HOST = "localhost";
	private final static int CASSANDRA_NATIVE_PORT = 9942;
	private final static int CASSANDRA_THRIFT_PORT = 9960;

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(
			"cassandraPropertyValueProvider-cql-dataload.cql", KEYSPACE_NAME), CASSANDRA_CONFIG, CASSANDRA_HOST,
			CASSANDRA_NATIVE_PORT);

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {

		EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG);

		/*
		 * Load data file to creat the test keyspace before we init the template
		 */
		DataLoader dataLoader = new DataLoader("Test Cluster", CASSANDRA_HOST + ":" + CASSANDRA_THRIFT_PORT);
		dataLoader.load(new ClassPathYamlDataSet("cassandra-keyspace.yaml"));
	}

	@Test
	public void basicNullsTest() {

		cqlOperations.getUpdateOperation("insert into test.basic_types_table (id) values ('nulls')").execute();

		BasicTypesEntity nullProps = cassandraOperations.getFindByIdOperation(BasicTypesEntity.class, "nulls").execute();
		assertThat(nullProps, is(not(nullValue(BasicTypesEntity.class))));
		assertThat(nullProps.getId(), is("nulls"));
		assertThat(nullProps.getPropascii(), is(nullValue()));
		assertThat(nullProps.getPropbigint(), is(nullValue()));
		assertThat(nullProps.getPropblob(), is(nullValue()));
		assertThat(nullProps.getPropboolean(), is(nullValue()));
		assertThat(nullProps.getPropdecimal(), is(nullValue()));
		assertThat(nullProps.getPropdouble(), is(nullValue()));
		assertThat(nullProps.getPropfloat(), is(nullValue()));
		assertThat(nullProps.getPropinet(), is(nullValue()));
		assertThat(nullProps.getPropint(), is(nullValue()));
		assertThat(nullProps.getProptext(), is(nullValue()));
		assertThat(nullProps.getProptimestamp(), is(nullValue()));
		assertThat(nullProps.getPropuuid(), is(nullValue()));
		assertThat(nullProps.getProptimeuuid(), is(nullValue()));
		assertThat(nullProps.getPropvarchar(), is(nullValue()));
		assertThat(nullProps.getPropvarint(), is(nullValue()));
	}

	@Test
	public void basicValuesReadTest() throws Exception {

		UUID uuid = UUID.fromString("f7a04220-6dda-11e3-981f-0800200c9a66");
		long longValue = 555555555555L;

		cqlOperations.getUpdateOperation(
				String.format("insert into test.basic_types_table (id, propascii, propbigint, propblob, propboolean, "
						+ "propdecimal, propdouble, propfloat, propinet, propint, proptext, proptimestamp, propuuid, "
						+ "proptimeuuid, propvarchar, propvarint) "
						+ "values ('ascii', 'ascii test value', %d, 0xcafebabe, true, 10.56, 1.5E50, 1E9, '10.1.2.3', "
						+ "123456, 'text test value', '2013-12-25T12:34:00+0000', %s, %s, 'varchar test value', "
						+ "12345678901234567890)", longValue, uuid.toString(), uuid.toString())).execute();
		BasicTypesEntity entity = cassandraOperations.getFindByIdOperation(BasicTypesEntity.class, "ascii").execute();
		assertThat(entity, is(not(nullValue(BasicTypesEntity.class))));
		assertThat(entity.getId(), is("ascii"));
		assertThat(entity.getPropascii(), is("ascii test value"));
		assertThat(entity.getPropboolean(), is(true));
		assertThat(entity.getPropdecimal(), BigDecimalCloseTo.closeTo(new BigDecimal(10.56), new BigDecimal(0.001)));
		assertThat(entity.getPropdouble(), IsCloseTo.closeTo(1.5E50, 0.001));
		assertThat(entity.getPropfloat().doubleValue(), IsCloseTo.closeTo(1E9, 1.));
		assertThat(entity.getPropinet(), is(Inet4Address.getByName("10.1.2.3")));
		assertThat(entity.getPropint(), is(123456));
		assertThat(entity.getProptext(), is("text test value"));
		Date expectedDate = ISODateTimeFormat.dateTime().parseDateTime("2013-12-25T12:34:00.000+00:00").toDate();
		assertThat(entity.getProptimestamp(), is(expectedDate));
		assertThat(entity.getPropuuid(), is(uuid));
		assertThat(entity.getProptimeuuid(), is(uuid));
		assertThat(entity.getPropvarchar(), is("varchar test value"));
		assertThat(entity.getPropvarint(), is(new BigInteger("12345678901234567890")));
	}

	@Test
	public void basicValuesWriteTest() throws Exception {

		ByteBuffer blob = ByteBuffer.wrap(new byte[] { (byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe });
		UUID uuid = UUID.fromString("f7a04220-6dda-11e3-981f-0800200c9a66");
		long longValue = 555555555555L;
		InetAddress inetaddr = Inet4Address.getByName("10.1.2.3");
		Date date = ISODateTimeFormat.dateTime().parseDateTime("2013-12-25T12:34:00.000+00:00").toDate();

		BasicTypesEntity newEntity = new BasicTypesEntity();
		newEntity.setId("ascii");
		newEntity.setPropascii("ascii test value");
		newEntity.setPropbigint(longValue);
		newEntity.setPropblob(blob);
		newEntity.setPropboolean(true);
		newEntity.setPropdecimal(new BigDecimal(10.56));
		newEntity.setPropdouble(1.5E50);
		newEntity.setPropfloat(1E9F);
		newEntity.setPropinet(inetaddr);
		newEntity.setPropint(123456);
		newEntity.setProptext("text test value");
		newEntity.setProptimestamp(date);
		newEntity.setPropuuid(uuid);
		newEntity.setProptimeuuid(uuid);
		newEntity.setPropvarchar("varchar test value");
		newEntity.setPropvarint(new BigInteger("12345678901234567890"));
		cassandraOperations.getSaveNewOperation(newEntity).execute();

		BasicTypesEntity entity = cassandraOperations.getFindByIdOperation(BasicTypesEntity.class, "ascii").execute();
		assertThat(entity, is(not(nullValue(BasicTypesEntity.class))));
		assertThat(entity.getId(), is("ascii"));
		assertThat(entity.getPropascii(), is("ascii test value"));
		assertThat(entity.getPropboolean(), is(true));
		assertThat(entity.getPropdecimal(), BigDecimalCloseTo.closeTo(new BigDecimal(10.56), new BigDecimal(0.001)));
		assertThat(entity.getPropdouble(), IsCloseTo.closeTo(1.5E50, 0.001));
		assertThat(entity.getPropfloat().doubleValue(), IsCloseTo.closeTo(1E9, 1.));
		assertThat(entity.getPropinet(), is(inetaddr));
		assertThat(entity.getPropint(), is(123456));
		assertThat(entity.getProptext(), is("text test value"));
		assertThat(entity.getProptimestamp(), is(date));
		assertThat(entity.getPropuuid(), is(uuid));
		assertThat(entity.getProptimeuuid(), is(uuid));
		assertThat(entity.getPropvarchar(), is("varchar test value"));
		assertThat(entity.getPropvarint(), is(new BigInteger("12345678901234567890")));
	}

	@Test
	public void collectionsNullsTest() {

		cqlOperations.getUpdateOperation("insert into test.collection_types_table (id) values ('nulls')").execute();

		CollectionTypesEntity nullProps = cassandraOperations.getFindByIdOperation(CollectionTypesEntity.class, "nulls")
				.execute();
		assertThat(nullProps, is(not(nullValue(CollectionTypesEntity.class))));
		assertThat(nullProps.getId(), is("nulls"));
		assertThat(nullProps.getTextlist(), is(nullValue()));
		assertThat(nullProps.getTextmap(), is(nullValue()));
		assertThat(nullProps.getTextset(), is(nullValue()));
		assertThat(nullProps.getTextuuidmap(), is(nullValue()));
		assertThat(nullProps.getUuidlist(), is(nullValue()));
		assertThat(nullProps.getUuidset(), is(nullValue()));
	}

	@Test
	public void collectionsValuesReadTest() {

		UUID mapUUID = UUID.fromString("f7a04220-6dda-11e3-981f-0800200c9a66");
		UUID listUUID[] = { UUID.fromString("149f0c80-6ddb-11e3-981f-0800200c9a66"),
				UUID.fromString("149f0c81-6ddb-11e3-981f-0800200c9a66") };
		UUID setUUID = UUID.fromString("149f0c82-6ddb-11e3-981f-0800200c9a66");

		cqlOperations.getUpdateOperation(
				String.format("insert into test.collection_types_table "
						+ "(id, textlist, textmap, textset, textuuidmap, uuidlist, uuidset) "
						+ "values ('values', ['text1', 'text2'], {'key1':'value1', 'key2':'value2'}, "
						+ "{'settext1', 'settext2'}, {'uuid1':%s}, " + "[%s, %s], " + "{%s} )", mapUUID.toString(),
						listUUID[0].toString(), listUUID[1].toString(), setUUID.toString())).execute();

		CollectionTypesEntity entity = cassandraOperations.getFindByIdOperation(CollectionTypesEntity.class, "values")
				.execute();

		assertThat(entity, is(not(nullValue(CollectionTypesEntity.class))));
		assertThat(entity.getId(), is("values"));

		List<String> textList = entity.getTextlist();
		assertThat(textList.size(), is(2));
		assertThat(textList.get(0), is("text1"));
		assertThat(textList.get(1), is("text2"));

		Map<String, String> textMap = entity.getTextmap();
		assertThat(textMap.size(), is(2));
		assertThat(textMap.get("key1"), is("value1"));
		assertThat(textMap.get("key2"), is("value2"));

		Set<String> textSet = entity.getTextset();
		assertThat(textSet.size(), is(2));
		assertThat(textSet.contains("settext1"), is(true));
		assertThat(textSet.contains("settext2"), is(true));

		Map<String, UUID> textuuidMap = entity.getTextuuidmap();
		assertThat(textuuidMap.size(), is(1));
		assertThat(textuuidMap.get("uuid1"), is(mapUUID));

		List<UUID> uuidList = entity.getUuidlist();
		for (int i = 0; i <= 1; ++i) {
			assertThat(uuidList.get(i), is(listUUID[i]));
		}

		Set<UUID> uuidSet = entity.getUuidset();
		assertThat(uuidSet.contains(setUUID), is(true));
	}

	@Test
	public void collectionsValuesWriteTest() {

		UUID mapUUID = UUID.fromString("f7a04220-6dda-11e3-981f-0800200c9a66");
		UUID listUUID[] = { UUID.fromString("149f0c80-6ddb-11e3-981f-0800200c9a66"),
				UUID.fromString("149f0c81-6ddb-11e3-981f-0800200c9a66") };
		UUID setUUID = UUID.fromString("149f0c82-6ddb-11e3-981f-0800200c9a66");

		CollectionTypesEntity newEntity = new CollectionTypesEntity();
		newEntity.setId("values");
		newEntity.setTextlist(ImmutableList.of("text1", "text2"));
		newEntity.setTextmap(ImmutableMap.of("key1", "value1", "key2", "value2"));
		newEntity.setTextset(ImmutableSet.of("settext1", "settext2"));
		newEntity.setTextuuidmap(ImmutableMap.of("uuid1", mapUUID));
		newEntity.setUuidlist(ImmutableList.of(listUUID[0], listUUID[1]));
		newEntity.setUuidset(ImmutableSet.of(setUUID));

		cassandraOperations.getSaveNewOperation(newEntity).execute();

		CollectionTypesEntity entity = cassandraOperations.getFindByIdOperation(CollectionTypesEntity.class, "values")
				.execute();

		assertThat(entity, is(not(nullValue(CollectionTypesEntity.class))));
		assertThat(entity.getId(), is("values"));

		List<String> textList = entity.getTextlist();
		assertThat(textList.size(), is(2));
		assertThat(textList.get(0), is("text1"));
		assertThat(textList.get(1), is("text2"));

		Map<String, String> textMap = entity.getTextmap();
		assertThat(textMap.size(), is(2));
		assertThat(textMap.get("key1"), is("value1"));
		assertThat(textMap.get("key2"), is("value2"));

		Set<String> textSet = entity.getTextset();
		assertThat(textSet.size(), is(2));
		assertThat(textSet.contains("settext1"), is(true));
		assertThat(textSet.contains("settext2"), is(true));

		Map<String, UUID> textuuidMap = entity.getTextuuidmap();
		assertThat(textuuidMap.size(), is(1));
		assertThat(textuuidMap.get("uuid1"), is(mapUUID));

		List<UUID> uuidList = entity.getUuidlist();
		for (int i = 0; i <= 1; ++i) {
			assertThat(uuidList.get(i), is(listUUID[i]));
		}

		Set<UUID> uuidSet = entity.getUuidset();
		assertThat(uuidSet.contains(setUUID), is(true));
	}

	@Test
	public void embeddedIdReadTest() {

		cqlOperations.getUpdateOperation(
				"insert into test.embedded_id_table (partitionkey, clusteringkey, proptext) " + " values (1, 'first', 'one')")
				.execute();

		EmbeddedIdEntity.PK pk = new EmbeddedIdEntity.PK(1, "first");
		EmbeddedIdEntity entity = cassandraOperations.getFindByIdOperation(EmbeddedIdEntity.class, pk).execute();
		assertThat(entity, is(not(nullValue(EmbeddedIdEntity.class))));
		EmbeddedIdEntity.PK entityPk = entity.getId();
		assertThat(entityPk, is(not(nullValue(EmbeddedIdEntity.PK.class))));
		assertThat(entityPk.getPartitionKey(), equalTo(pk.getPartitionKey()));
		assertThat(entityPk.getClusteringKey(), equalTo(pk.getClusteringKey()));
		assertThat(entity.getProptext(), equalTo("one"));
	}

	@Test
	public void embeddedIdInsertTest() {

		EmbeddedIdEntity.PK pk = new EmbeddedIdEntity.PK(2, "second");
		EmbeddedIdEntity newEntity = new EmbeddedIdEntity();
		newEntity.setId(pk);
		newEntity.setProptext("two");
		cassandraOperations.getSaveNewOperation(newEntity).execute();

		EmbeddedIdEntity entity = cassandraOperations.getFindByIdOperation(EmbeddedIdEntity.class, pk).execute();
		assertThat(entity, is(not(nullValue(EmbeddedIdEntity.class))));
		EmbeddedIdEntity.PK entityPk = entity.getId();
		assertThat(entityPk, is(not(nullValue(EmbeddedIdEntity.PK.class))));
		assertThat(entityPk.getPartitionKey(), equalTo(pk.getPartitionKey()));
		assertThat(entityPk.getClusteringKey(), equalTo(pk.getClusteringKey()));
		assertThat(entity.getProptext(), equalTo("two"));
	}

	@Test
	public void embeddedIdUpdateTest() {

		EmbeddedIdEntity.PK pk = new EmbeddedIdEntity.PK(2, "second");

		cqlOperations.getUpdateOperation(
				"insert into test.embedded_id_table (partitionkey, clusteringkey, proptext) " + " values (2, 'second', 'two')")
				.execute();

		EmbeddedIdEntity entity = cassandraOperations.getFindByIdOperation(EmbeddedIdEntity.class, pk).execute();
		assertThat(entity, is(not(nullValue(EmbeddedIdEntity.class))));
		EmbeddedIdEntity.PK entityPk = entity.getId();
		assertThat(entityPk, is(not(nullValue(EmbeddedIdEntity.PK.class))));
		assertThat(entityPk.getPartitionKey(), equalTo(pk.getPartitionKey()));
		assertThat(entityPk.getClusteringKey(), equalTo(pk.getClusteringKey()));
		assertThat(entity.getProptext(), equalTo("two"));

		entity.setProptext("Two!");
		cassandraOperations.getSaveOperation(entity).execute();
		entity = cassandraOperations.getFindByIdOperation(EmbeddedIdEntity.class, pk).execute();
		assertThat(entity.getProptext(), equalTo("Two!"));
	}

	@Test
	public void embeddedIdDeleteTest() {

		EmbeddedIdEntity.PK pk = new EmbeddedIdEntity.PK(2, "second");

		cqlOperations.getUpdateOperation(
				"insert into test.embedded_id_table (partitionkey, clusteringkey, proptext) " + " values (2, 'second', 'two')")
				.execute();

		EmbeddedIdEntity entity = cassandraOperations.getFindByIdOperation(EmbeddedIdEntity.class, pk).execute();
		assertThat(entity, is(not(nullValue(EmbeddedIdEntity.class))));
		EmbeddedIdEntity.PK entityPk = entity.getId();
		assertThat(entityPk, is(not(nullValue(EmbeddedIdEntity.PK.class))));
		assertThat(entityPk.getPartitionKey(), equalTo(pk.getPartitionKey()));
		assertThat(entityPk.getClusteringKey(), equalTo(pk.getClusteringKey()));
		assertThat(entity.getProptext(), equalTo("two"));

		cassandraOperations.getDeleteOperation(entity).execute();
		entity = cassandraOperations.getFindByIdOperation(EmbeddedIdEntity.class, pk).execute();
		assertThat(entity, is(nullValue(EmbeddedIdEntity.class)));
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	@SuppressWarnings("deprecation")
	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}
}
