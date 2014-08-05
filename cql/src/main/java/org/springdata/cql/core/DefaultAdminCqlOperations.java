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

import org.springdata.cql.generator.AlterKeyspaceCqlGenerator;
import org.springdata.cql.generator.CreateKeyspaceCqlGenerator;
import org.springdata.cql.generator.DropKeyspaceCqlGenerator;
import org.springdata.cql.generator.UseKeyspaceCqlGenerator;
import org.springdata.cql.option.KeyspaceOptions;
import org.springdata.cql.spec.AlterKeyspaceSpecification;
import org.springdata.cql.spec.CreateKeyspaceSpecification;
import org.springdata.cql.spec.DropKeyspaceSpecification;
import org.springdata.cql.spec.UseKeyspaceSpecification;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

/**
 * Default implementation of {@link AdminCqlOperations}.
 * 
 * @author Alex Shvid
 */
public class DefaultAdminCqlOperations implements AdminCqlOperations {

	private static final String SYSTEM_KEYSPACE = "system";

	private final CqlTemplate cqlTemplate;

	protected DefaultAdminCqlOperations(CqlTemplate cqlTemplate) {
		Assert.notNull(cqlTemplate);

		this.cqlTemplate = cqlTemplate;
	}

	@Override
	public UpdateOperation createKeyspace(String keyspace, final KeyspaceOptions keyspaceOptions) {

		Assert.notNull(keyspace);
		Assert.notNull(keyspaceOptions);

		CreateKeyspaceSpecification spec = new CreateKeyspaceSpecification().name(keyspace).with(
				keyspaceOptions.getOptions());

		return createKeyspace(spec);

	}

	@Override
	public UpdateOperation createKeyspace(CreateKeyspaceSpecification spec) {

		Assert.notNull(spec);

		CreateKeyspaceCqlGenerator generator = new CreateKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation alterKeyspace(String keyspace, KeyspaceOptions keyspaceOptions) {

		Assert.notNull(keyspace);
		Assert.notNull(keyspaceOptions);

		AlterKeyspaceSpecification spec = new AlterKeyspaceSpecification().name(keyspace)
				.with(keyspaceOptions.getOptions());

		return alterKeyspace(spec);

	}

	@Override
	public UpdateOperation alterKeyspace(AlterKeyspaceSpecification spec) {

		Assert.notNull(spec);

		AlterKeyspaceCqlGenerator generator = new AlterKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation dropKeyspace(String keyspace) {

		Assert.notNull(keyspace);

		DropKeyspaceSpecification spec = new DropKeyspaceSpecification().name(keyspace);

		return dropKeyspace(spec);

	}

	@Override
	public UpdateOperation dropKeyspace(DropKeyspaceSpecification spec) {

		Assert.notNull(spec);

		DropKeyspaceCqlGenerator generator = new DropKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation useKeyspace(String keyspace) {

		Assert.notNull(keyspace);

		UseKeyspaceSpecification spec = new UseKeyspaceSpecification().name(keyspace);

		return useKeyspace(spec);

	}

	@Override
	public UpdateOperation useKeyspace(UseKeyspaceSpecification spec) {

		Assert.notNull(spec);

		UseKeyspaceCqlGenerator generator = new UseKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation useSystemKeyspace() {

		UseKeyspaceSpecification spec = new UseKeyspaceSpecification().name(SYSTEM_KEYSPACE);
		UseKeyspaceCqlGenerator generator = new UseKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cqlTemplate, generator.toCql());

	}

	/**
	 * Get the given keyspace metadata.
	 * 
	 * @param keyspace The name of the table.
	 */
	@Override
	public KeyspaceMetadata getKeyspaceMetadata(final String keyspace) {

		Assert.notNull(keyspace);

		return cqlTemplate.doExecute(new SessionCallback<KeyspaceMetadata>() {

			public KeyspaceMetadata doInSession(Session s) {

				return s.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase());
			}
		});

	}

}
