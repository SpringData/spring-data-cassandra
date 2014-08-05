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
package org.springdata.cassandra.config.xml;

import org.springdata.cassandra.config.CassandraConstants;
import org.springdata.cql.config.xml.CqlClusterParser;

/**
 * Parser for Cluster definitions.
 * 
 * @author Alex Shvid
 */

public class CassandraClusterParser extends CqlClusterParser {

	public CassandraClusterParser() {
		super(CassandraConstants.CASSANDRA_CLUSTER);
	}

}
