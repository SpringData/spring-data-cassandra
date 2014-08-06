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
package org.springdata.cql.core;

import org.springdata.cql.option.KeyspaceOptions;
import org.springdata.cql.spec.AlterKeyspaceSpecification;
import org.springdata.cql.spec.CreateKeyspaceSpecification;
import org.springdata.cql.spec.DropKeyspaceSpecification;
import org.springdata.cql.spec.UseKeyspaceSpecification;

import com.datastax.driver.core.KeyspaceMetadata;

/**
 * Operations for managing a Cassandra keyspace.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew T. Adams
 */
public interface AdminCqlOperations {

	/**
	 * Creates Keyspace with given options
	 * 
	 * @param keyspace The keyspace name
	 * @param keyspaceOptions Keyspace options.
	 */
	ExecuteOperation createKeyspace(String keyspace, KeyspaceOptions keyspaceOptions);

	/**
	 * Creates keyspace with a given CreateKeyspaceSpecification
	 * 
	 * @param createKeyspaceSpecification
	 * @return
	 */
	ExecuteOperation createKeyspace(CreateKeyspaceSpecification createKeyspaceSpecification);

	/**
	 * Alters Keyspace with given name and options
	 * 
	 * @param keyspace The keyspace name
	 * @param keyspaceOptions Keyspace options.
	 */
	ExecuteOperation alterKeyspace(String keyspace, KeyspaceOptions keyspaceOptions);

	/**
	 * Alters keyspace with a given AlterKeyspaceSpecification
	 * 
	 * @param alterKeyspaceSpecification
	 * @return
	 */
	ExecuteOperation alterKeyspace(AlterKeyspaceSpecification alterKeyspaceSpecification);

	/**
	 * Drops keyspace with given keyspace name
	 * 
	 * @param keyspace The keyspace name
	 * 
	 */
	ExecuteOperation dropKeyspace(String keyspace);

	/**
	 * Drops keyspace with a given DropKeyspaceSpecification
	 * 
	 * @param dropKeyspaceSpecification
	 * @return
	 */
	ExecuteOperation dropKeyspace(DropKeyspaceSpecification dropKeyspaceSpecification);

	/**
	 * Use keyspace
	 * 
	 * @param keyspace The keyspace name
	 * 
	 */
	ExecuteOperation useKeyspace(String keyspace);

	/**
	 * Use keyspace with a given UseKeyspaceSpecification
	 * 
	 * @param useKeyspaceSpecification
	 * @return
	 */
	ExecuteOperation useKeyspace(UseKeyspaceSpecification useKeyspaceSpecification);

	/**
	 * Use system keyspace
	 * 
	 * @param optionsOrNull The Execute Options Object if exists
	 * 
	 */
	ExecuteOperation useSystemKeyspace();

	/**
	 * Gets the keyspace metadata.
	 */
	KeyspaceMetadata getKeyspaceMetadata(String keyspace);

}
