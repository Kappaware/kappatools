package com.kappaware.k2cassandra.cassandra;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.utils.UUIDs;
import com.kappaware.k2cassandra.cassandra.DbEngine.DbTable;

public class DbTableImpl implements DbTable {
	static Logger log = LoggerFactory.getLogger(DbTableImpl.class);

	private String name;
	private Session session;
	private TableMetadata metadata;
	private PreparedStatement writePreparedStatement = null;
	private Set<String> partitionKeyFields = new HashSet<String>();
	private Set<String> warnedField = new HashSet<String>();

	public DbTableImpl(String name, Session session, TableMetadata tmd) {
		this.name = name;
		this.session = session;
		this.metadata = tmd;
		for (ColumnMetadata cmd : tmd.getPartitionKey()) {
			this.partitionKeyFields.add(cmd.getName());
		}
	}

	private PreparedStatement getWritePreparedStatement() {
		if (this.writePreparedStatement == null) {
			StringBuffer fieldList = new StringBuffer();
			StringBuffer valueList = new StringBuffer();
			String sep = "";
			for (ColumnMetadata cmd : this.metadata.getColumns()) {
				fieldList.append(sep);
				valueList.append(sep);
				fieldList.append(cmd.getName());
				valueList.append("?");
				sep = ", ";
			}
			String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", this.name, fieldList, valueList);
			log.debug(String.format("Build prepared statement: '%s'", sql));
			this.writePreparedStatement = this.session.prepare(sql);
		}
		return writePreparedStatement;
	}

	/*
	 * This write is based on the table definition. This has some drawback:
	 * - We need to manually detect orphan json fields.
	 * - In case of update, missing field in request are nulled.
	 * - For null value in case of field part of partition key, we generate a random value for UUID field. Other type will generate an error
	 * 
	 * Also, note we write synchronously. 
	 * Speedup may be achieved by using asynchronous call, but we would need to setup some 'back pressure' handling to avoid pending request overflow
	 */
	@Override
	public void write(Map<String, Object> record) throws DbEngineException {
		Set<String> usedFields = new HashSet<String>();
		PreparedStatement ps = this.getWritePreparedStatement();
		BoundStatement bs = new BoundStatement(ps);
		for (ColumnDefinitions.Definition colDef : ps.getVariables().asList()) {
			Object o = record.get(colDef.getName());
			if (o != null) {
				setTypedValue(bs, colDef, o);
				usedFields.add(colDef.getName());
			} else {
				if (this.partitionKeyFields.contains(colDef.getName())) {
					// Must be generated
					generateValue(bs, colDef);
				} else {
					bs.setToNull(colDef.getName());
				}
			}
		}
		for (String f : record.keySet()) {
			if (!usedFields.contains(f) && !this.warnedField.contains(f)) {
				log.warn(String.format("Field '%s' does not exists in table '%s'", f, this.name));
				this.warnedField.add(f);
			}
		}
		this.session.execute(bs);
	}

	private void generateValue(BoundStatement bs, Definition colDef) throws DbEngineException {
		switch (colDef.getType().getName()) {
			case UUID:
				bs.setUUID(colDef.getName(), UUIDs.random());
			break;
			case TIMEUUID:
				bs.setUUID(colDef.getName(), UUIDs.timeBased());
			break;
			case VARCHAR:
			case TEXT:
			case ASCII:
			case BIGINT:
			case COUNTER:
			case BOOLEAN:
			case DECIMAL:
			case DOUBLE:
			case FLOAT:
			case INET:
			case INT:
			case TIMESTAMP:
			case VARINT:
			case TUPLE:
			case UDT:
			case BLOB:
			case CUSTOM:
			case LIST:
			case MAP:
			case SET:
			default:
				throw new DbEngineException(String.format("Unable to generate a value for a field of type %s (field '%s'). A value must be provided", colDef.getType().getName().toString(), colDef.getName()));
		}
	}

	private void setTypedValue(BoundStatement bs, Definition colDef, Object value) throws DbEngineException {
		switch (colDef.getType().getName()) {
			case VARCHAR:
			case TEXT:
			case ASCII:
				bs.setString(colDef.getName(), value.toString());
			break;
			case UUID:
			case TIMEUUID:
				bs.setUUID(colDef.getName(), (value instanceof UUID) ? (UUID) value : UUID.fromString(value.toString()));
			break;
			case BIGINT:
			case COUNTER:
				bs.setLong(colDef.getName(), (value instanceof Long) ? (Long) value : new Long(value.toString()));
			break;
			case BOOLEAN:
				bs.setBool(colDef.getName(), (value instanceof Boolean) ? (Boolean) value : new Boolean(value.toString()));
			break;
			case DECIMAL:
				bs.setDecimal(colDef.getName(), (value instanceof BigDecimal) ? (BigDecimal) value : new BigDecimal(value.toString()));
			break;
			case DOUBLE:
				bs.setDouble(colDef.getName(), (value instanceof Double) ? (Float) value : new Double(value.toString()));
			break;
			case FLOAT:
				bs.setFloat(colDef.getName(), (value instanceof Float) ? (Float) value : new Float(value.toString()));
			break;
			case INET:
				try {
					bs.setInet(colDef.getName(), (value instanceof InetAddress) ? (InetAddress) value : InetAddress.getByName(value.toString()));
				} catch (UnknownHostException e) {
					throw new DbEngineException(String.format("Unable to convert object '%s' of class '%s' to a valid InetAddr (field '%s')", value.toString(), value.getClass().getName(), colDef.getName()));
				}
			break;
			case INT:
				bs.setInt(colDef.getName(), (value instanceof Integer) ? (Integer) value : new Integer(value.toString()));
			break;
			case TIMESTAMP:
				bs.setTimestamp(colDef.getName(), toDate(value, colDef.getName()));
			break;
			case VARINT:
				bs.setVarint(colDef.getName(), (value instanceof BigInteger) ? (BigInteger) value : new BigInteger(value.toString()));
			break;
			case TUPLE:
			case UDT:
			case BLOB:
			case CUSTOM:
			case LIST:
			case MAP:
			case SET:
			default:
				throw new RuntimeException(String.format("Unhandled Cassandra DataType: %s (field '%s')", colDef.getType().getName().toString(), colDef.getName()));
		}
	}

	private java.util.Date toDate(Object value, String fieldName) throws DbEngineException {
		if (value instanceof String) {
			return new java.util.Date(parseStringDate((String) value).getTimeInMillis());
		} else if (value instanceof Number) {
			return new java.sql.Date(((Number) value).longValue());
		} else if (value instanceof java.util.Date) {
			return (java.util.Date) value;
		} else {
			throw new DbEngineException(String.format("Unable to convert object '%s' of class '%s' to a valid Date (field '%s')", value.toString(), value.getClass().getName(), fieldName));
		}
	}

	/*
	static public void main(String[] argc) {
		Long now = System.currentTimeMillis();
		String s1 = Utils.printIsoDateTime(now);
		System.out.print(String.format("s1:%s    s2:%s\n", s1, s2));
	}*/

	public static Calendar parseStringDate(String date) throws DbEngineException {
		try {
			if (date.length() > 13) {
				return DatatypeConverter.parseDateTime(date);
			} else {
				return DatatypeConverter.parseDate(date);
			}
		} catch (IllegalArgumentException e) {
			throw new DbEngineException(String.format("Unable to convert string '%s' to a valid Date", date));
		}
	}

}
