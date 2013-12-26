package org.springdata.cassandra.data.test.integration.convert;

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
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.base.core.CassandraOperations;
import org.springdata.cassandra.data.core.CassandraDataOperations;
import org.springdata.cassandra.data.test.integration.config.JavaConfig;
import org.springdata.cassandra.data.test.integration.table.BasicTypesEntity;
import org.springdata.cassandra.data.test.integration.table.CollectionTypesEntity;
import org.springdata.cassandra.data.test.integration.table.EmbeddedIdEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.text.DateFormat;
import java.util.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Named;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Tests for data conversion related classes
 * 
 * @author Dzmitry Zhemchuhou
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { JavaConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraDataConversionTests {

	@Autowired
	@Named("cassandraTemplate")
	private CassandraOperations cassandraOperations;

	@Autowired
	private CassandraDataOperations cassandraDataTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraDataConversionTests.class);

	private final static String CASSANDRA_CONFIG = "cassandra.yaml";
	private final static String KEYSPACE_NAME = "test";
	private final static String CASSANDRA_HOST = "localhost";
	private final static int CASSANDRA_NATIVE_PORT = 9042;
	private final static int CASSANDRA_THRIFT_PORT = 9160;

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

		cassandraOperations.execute(false, "insert into test.basic_types_table (id) values ('nulls')", null);

		BasicTypesEntity nullProps = cassandraDataTemplate.findById("nulls", BasicTypesEntity.class, null);
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
	public void basicValuesTest() throws Exception {

		UUID uuid = UUID.fromString("f7a04220-6dda-11e3-981f-0800200c9a66");
		long longValue = 555555555555L;

		cassandraOperations.execute(false, String.format(
				"insert into test.basic_types_table (id, propascii, propbigint, propblob, propboolean, "
						+ "propdecimal, propdouble, propfloat, propinet, propint, proptext, proptimestamp, propuuid, "
						+ "proptimeuuid, propvarchar, propvarint) "
						+ "values ('ascii', 'ascii test value', %d, 0xcafebabe, true, 10.56, 1.5E50, 1E9, '10.1.2.3', "
						+ "123456, 'text test value', '2013-12-25T12:34:00+0000', %s, %s, 'varchar test value', "
						+ "12345678901234567890)", longValue, uuid.toString(), uuid.toString()), null);
		BasicTypesEntity entity = cassandraDataTemplate.findById("ascii", BasicTypesEntity.class, null);
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
	public void collectionsNullsTest() {

		cassandraOperations.execute(false, "insert into test.collection_types_table (id) values ('nulls')", null);

		CollectionTypesEntity nullProps = cassandraDataTemplate.findById("nulls", CollectionTypesEntity.class, null);
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
	public void collectionsValuesTest() {

		UUID mapUUID = UUID.fromString("f7a04220-6dda-11e3-981f-0800200c9a66");
		UUID listUUID[] = { UUID.fromString("149f0c80-6ddb-11e3-981f-0800200c9a66"),
				UUID.fromString("149f0c81-6ddb-11e3-981f-0800200c9a66") };
		UUID setUUID = UUID.fromString("149f0c82-6ddb-11e3-981f-0800200c9a66");

		cassandraOperations.execute(
				false,
				String.format("insert into test.collection_types_table "
						+ "(id, textlist, textmap, textset, textuuidmap, uuidlist, uuidset) "
						+ "values ('values', ['text1', 'text2'], {'key1':'value1', 'key2':'value2'}, "
						+ "{'settext1', 'settext2'}, {'uuid1':%s}, " + "[%s, %s], " + "{%s} )", mapUUID.toString(),
						listUUID[0].toString(), listUUID[1].toString(), setUUID.toString()), null);

		CollectionTypesEntity entity = cassandraDataTemplate.findById("values", CollectionTypesEntity.class, null);

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

		cassandraOperations.execute(false, "insert into test.embedded_id_table (partitionkey, clusteringkey, proptext) "
				+ " values (1, 'first', 'one')", null);

		EmbeddedIdEntity.PK pk = new EmbeddedIdEntity.PK(1, "first");
		EmbeddedIdEntity entity = cassandraDataTemplate.findById(pk, EmbeddedIdEntity.class, null);
		assertThat(entity, is(not(nullValue(EmbeddedIdEntity.class))));
		EmbeddedIdEntity.PK entityPk = entity.getId();
		assertThat(entityPk, is(not(nullValue(EmbeddedIdEntity.PK.class))));
		assertThat(entityPk.getPartitionKey(), equalTo(pk.getPartitionKey()));
		assertThat(entityPk.getClusteringKey(), equalTo(pk.getClusteringKey()));
		assertThat(entity.getProptext(), equalTo("one"));
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}
}
