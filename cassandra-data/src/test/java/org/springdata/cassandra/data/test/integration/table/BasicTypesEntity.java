package org.springdata.cassandra.data.test.integration.table;

import org.springdata.cassandra.data.mapping.Id;
import org.springdata.cassandra.data.mapping.Table;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

@Table(name = "basic_types_table")
public class BasicTypesEntity {

	@Id
	private String id;
	private String propascii;
	private Long propbigint;
	private ByteBuffer propblob;
	private Boolean propboolean;
	private BigDecimal propdecimal;
	private Double propdouble;
	private Float propfloat;
	private InetAddress propinet;
	private Integer propint;
	private String proptext;
	private Date proptimestamp;
	private UUID propuuid;
	private UUID proptimeuuid;
	private String propvarchar;
	private BigInteger propvarint;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPropascii() {
		return propascii;
	}

	public void setPropascii(String propascii) {
		this.propascii = propascii;
	}

	public Long getPropbigint() {
		return propbigint;
	}

	public void setPropbigint(Long propbigint) {
		this.propbigint = propbigint;
	}

	public ByteBuffer getPropblob() {
		return propblob;
	}

	public void setPropblob(ByteBuffer propblob) {
		this.propblob = propblob;
	}

	public Boolean getPropboolean() {
		return propboolean;
	}

	public void setPropboolean(Boolean propboolean) {
		this.propboolean = propboolean;
	}

	public BigDecimal getPropdecimal() {
		return propdecimal;
	}

	public void setPropdecimal(BigDecimal propdecimal) {
		this.propdecimal = propdecimal;
	}

	public Double getPropdouble() {
		return propdouble;
	}

	public void setPropdouble(Double propdouble) {
		this.propdouble = propdouble;
	}

	public Float getPropfloat() {
		return propfloat;
	}

	public void setPropfloat(Float propfloat) {
		this.propfloat = propfloat;
	}

	public InetAddress getPropinet() {
		return propinet;
	}

	public void setPropinet(InetAddress propinet) {
		this.propinet = propinet;
	}

	public Integer getPropint() {
		return propint;
	}

	public void setPropint(Integer propint) {
		this.propint = propint;
	}

	public String getProptext() {
		return proptext;
	}

	public void setProptext(String proptext) {
		this.proptext = proptext;
	}

	public Date getProptimestamp() {
		return proptimestamp;
	}

	public void setProptimestamp(Date proptimestamp) {
		this.proptimestamp = proptimestamp;
	}

	public UUID getPropuuid() {
		return propuuid;
	}

	public void setPropuuid(UUID propuuid) {
		this.propuuid = propuuid;
	}

	public UUID getProptimeuuid() {
		return proptimeuuid;
	}

	public void setProptimeuuid(UUID proptimeuuid) {
		this.proptimeuuid = proptimeuuid;
	}

	public String getPropvarchar() {
		return propvarchar;
	}

	public void setPropvarchar(String propvarchar) {
		this.propvarchar = propvarchar;
	}

	public BigInteger getPropvarint() {
		return propvarint;
	}

	public void setPropvarint(BigInteger propvarint) {
		this.propvarint = propvarint;
	}
}
