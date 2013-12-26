package org.springdata.cassandra.data.test.integration.table;

import com.datastax.driver.core.DataType;
import org.springdata.cassandra.data.mapping.Id;
import org.springdata.cassandra.data.mapping.Qualify;
import org.springdata.cassandra.data.mapping.Table;

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
