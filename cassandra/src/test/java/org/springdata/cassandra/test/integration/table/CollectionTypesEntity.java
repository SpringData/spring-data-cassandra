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
package org.springdata.cassandra.test.integration.table;

import com.datastax.driver.core.DataType;

import org.springdata.cassandra.mapping.Id;
import org.springdata.cassandra.mapping.Qualify;
import org.springdata.cassandra.mapping.Table;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Table(name = "collection_types_table")
public class CollectionTypesEntity {

	@Id
	private String id;

	@Qualify(type = DataType.Name.LIST, typeArguments = { DataType.Name.TEXT })
	private List<String> textlist;

	@Qualify(type = DataType.Name.MAP, typeArguments = { DataType.Name.TEXT, DataType.Name.TEXT })
	private Map<String, String> textmap;

	@Qualify(type = DataType.Name.SET, typeArguments = { DataType.Name.TEXT })
	private Set<String> textset;

	@Qualify(type = DataType.Name.LIST, typeArguments = { DataType.Name.UUID })
	private List<UUID> uuidlist;

	@Qualify(type = DataType.Name.MAP, typeArguments = { DataType.Name.TEXT, DataType.Name.UUID })
	private Map<String, UUID> textuuidmap;

	@Qualify(type = DataType.Name.SET, typeArguments = { DataType.Name.UUID })
	private Set<UUID> uuidset;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getTextlist() {
		return textlist;
	}

	public void setTextlist(List<String> textlist) {
		this.textlist = textlist;
	}

	public Map<String, String> getTextmap() {
		return textmap;
	}

	public void setTextmap(Map<String, String> textmap) {
		this.textmap = textmap;
	}

	public Set<String> getTextset() {
		return textset;
	}

	public void setTextset(Set<String> textset) {
		this.textset = textset;
	}

	public List<UUID> getUuidlist() {
		return uuidlist;
	}

	public void setUuidlist(List<UUID> uuidlist) {
		this.uuidlist = uuidlist;
	}

	public Map<String, UUID> getTextuuidmap() {
		return textuuidmap;
	}

	public void setTextuuidmap(Map<String, UUID> textuuidmap) {
		this.textuuidmap = textuuidmap;
	}

	public Set<UUID> getUuidset() {
		return uuidset;
	}

	public void setUuidset(Set<UUID> uuidset) {
		this.uuidset = uuidset;
	}
}
