package org.springdata.cassandra.data.test.integration.table;

import org.springdata.cassandra.base.core.KeyPart;
import org.springdata.cassandra.data.mapping.Embeddable;
import org.springdata.cassandra.data.mapping.EmbeddedId;
import org.springdata.cassandra.data.mapping.KeyColumn;
import org.springdata.cassandra.data.mapping.Table;

import java.io.Serializable;

@Table(name = "embedded_id_table")
public class EmbeddedIdEntity {

	@Embeddable
	public static class PK implements Serializable {

		@KeyColumn(keyPart = KeyPart.PARTITION, ordinal = 1)
		private int partitionKey;

		@KeyColumn(keyPart = KeyPart.CLUSTERING, ordinal = 1)
		private String clusteringKey;

		public PK() {
		}

		public PK(int partitionKey, String clusteringKey) {
			this.partitionKey = partitionKey;
			this.clusteringKey = clusteringKey;
		}

		public int getPartitionKey() {
			return partitionKey;
		}

		public void setPartitionKey(int partitionKey) {
			this.partitionKey = partitionKey;
		}

		public String getClusteringKey() {
			return clusteringKey;
		}

		public void setClusteringKey(String clusteringKey) {
			this.clusteringKey = clusteringKey;
		}
	}

	@EmbeddedId
	private PK id;
	private String proptext;

	public PK getId() {
		return id;
	}

	public void setId(PK id) {
		this.id = id;
	}

	public String getProptext() {
		return proptext;
	}

	public void setProptext(String proptext) {
		this.proptext = proptext;
	}
}
