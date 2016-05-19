package com.kappaware.k2jdbc.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.sql.DataSource;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.k2jdbc.jdbc.DbCatalog.DbColumnDef;
import com.kappaware.k2jdbc.jdbc.DbCatalog.DbTableDef;

public class DbEngineImpl implements DbEngine {
	static Logger log = LoggerFactory.getLogger(DbEngineImpl.class);

	private DataSource dataSource;
	private DbCatalog dbCatalog;
	

	public DbEngineImpl(DataSource dataSource) {
		this.dataSource = dataSource;
	}


	/*
	 * We need to rebuild a different preparedStatement for each record, as the only way of using column's default value is to NOT have the column is the definition
	 * (Or to set DEFAULT in place of ?, which also means different command for different records).
	 * - Setting a column to NULL (.setNull()) will force to NULL, despite the default.
	 * - Not setting the column at all generate an error.  
	 * 
	 * So, the sql command is to be built from the input data, not the table schema
	 * 
	 */
	@Override
	public void write(String tableName, List<Map<String, Object>> dataSet) throws SQLException, IOException {
		DbTableDef tableDef =  this.getDbCatalog().getTableDef(tableName);
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			connection = this.dataSource.getConnection();
			connection.setAutoCommit(false);
			int rowNum = 0;
			for (Map<String, Object> row : dataSet) {
				if (row.size() == 0) {
					log.warn(String.format("Row#%d is empty. Skipped", rowNum));
				} else {
					List<String> fieldNames = new Vector<String>(row.keySet()); // To fix field ordering
					StringBuffer fields = new StringBuffer();
					StringBuffer params = new StringBuffer();
					String sep = "";
					for (String fieldName : fieldNames) {
						DbColumnDef column = tableDef.getColumnDef(fieldName);
						if (column == null) {
							// A field is present in JSON, but not in DB. Forget it
							log.warn(String.format("Field '%s' does not exists in table '%s' (row# %d)", fieldName, tableDef.getName(), rowNum));
						}
						fields.append(sep);
						fields.append(column.getName());
						params.append(sep);
						params.append("?");
						sep = ", ";
					}
					String sql = String.format("INSERT INTO %s ( %s ) VALUES ( %s )", tableDef.getName(), fields.toString(), params.toString());
					statement = connection.prepareStatement(sql);
					int pos = 1;
					statement.clearParameters();
					for (String fieldName : fieldNames) {
						DbColumnDef column = tableDef.getColumnDef(fieldName);
						this.setStatementParameter(statement, pos++, column.getJdbcType(), row.get(fieldName), rowNum);
					}
					int x = statement.executeUpdate();
					if (x != 1) {
						throw new SQLException(String.format("Insert statement returned %d rows affected!! - Should be 1 (row# %d)", x, rowNum));
					}
					statement.close();
					statement = null;
				}
				rowNum++;
			}
			connection.commit();
		} catch (Throwable t) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException e) {
				}
			}
			throw t;
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}		
		
	}

	@Override
	public List<Map<String, Object>> query(String query, Object[] params) throws SQLException {
		Connection connection = null;
		PreparedStatement statement = null;
		List<Map<String, Object>> recordSet = null;
		try {
			connection = this.dataSource.getConnection();
			statement = connection.prepareStatement(query);
			statement.clearParameters();
			if(params != null) {
				for(int i = 0; i < params.length; i++) {
					statement.setObject(i + 1, params[i]);
				}
			}
			ResultSet rs = statement.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			recordSet = new Vector<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> record = new HashMap<String, Object>();
				for (int i = 1; i <= columnCount; i++) {
					Object o = getTypedValueFromResultSet(rs, i, rsmd);
					record.put(rsmd.getColumnName(i), o);
				}
				recordSet.add(record);
			}
			return recordSet;
		} finally {
			if (recordSet == null) {
				log.debug(String.format("Execute query: '%s' (params:[%s]): Failed", query, Arrays.toString(params)));
			} else {
				log.debug(String.format("Execute query: '%s' (params:[%s]): RecordSet.size:%d", query, Arrays.toString(params), recordSet.size()));
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	/**
	 * Excecute an SQL command whch return no reult set
	 * @param sql
	 * @return
	 * @throws SQLException
	 */

	@Override
	public int execute(String sql) throws SQLException {
		Connection connection = null;
		Statement statement = null;
		Integer r = null;
		try {
			connection = this.dataSource.getConnection();
			statement = connection.createStatement();
			r = statement.executeUpdate(sql);
			return r;
		} finally {
			if (r == null) {
				log.debug(String.format("Execute cmd: '%s': Failed", sql));
			} else {
				log.debug(String.format("Execute cmd: '%s': return:%d", sql, r));
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	@Override
	public void close() {
	}

	@Override
	public DbCatalog getDbCatalog() {
		if(this.dbCatalog == null) {
			this.dbCatalog = new DbCatalogImpl(this.dataSource);
		}
		return this.dbCatalog;
	}

	

	private void setStatementParameter(PreparedStatement statement, int pos, int jdbcType, Object value, int rowNum) throws SQLException {
		if (value == null) {
			// If value is null, we want to use the column defined default.
			// Should be some king of setDefault(). But setNull force a NULL, even if there is a DEFAULT on the column desc
			//statement.setNull(pos, jdbcType);
		} else {
			try {
				switch (jdbcType) {
					case Types.VARCHAR:
					case Types.CHAR:
						statement.setString(pos, value.toString());
					break;
					case Types.NUMERIC:
						statement.setBigDecimal(pos, (value instanceof BigDecimal) ? (BigDecimal) value : new BigDecimal(value.toString()));
					break;
					case Types.BIT:
						statement.setBoolean(pos, (value instanceof Boolean) ? (Boolean) value : new Boolean(value.toString()));
					break;
					case Types.INTEGER:
					case Types.TINYINT:
						statement.setInt(pos, (value instanceof Integer) ? (Integer) value : new Integer(value.toString()));
					break;
					case Types.BIGINT:
						statement.setLong(pos, (value instanceof Long) ? (Long) value : new Long(value.toString()));
					break;
					case Types.REAL:
						statement.setFloat(pos, (value instanceof Float) ? (Float) value : new Float(value.toString()));
					break;
					case Types.FLOAT:
						statement.setDouble(pos, (value instanceof Double) ? (Float) value : new Double(value.toString()));
					break;
					case Types.DATE:
						statement.setDate(pos, toSqlDate(value, rowNum));
					break;
					case Types.TIME:
						statement.setTime(pos, toSqlTime(value, rowNum));
					break;
					case Types.TIMESTAMP:
						statement.setTimestamp(pos, toSqlTimestamp(value, rowNum));
					break;
					case Types.BINARY:
					case Types.CLOB:
					case Types.BLOB:
					case Types.ARRAY:
					case Types.REF:
					default:
						throw new SQLException(String.format("Unhandled jdbcType: %d ", jdbcType));
						// break;
				}
			} catch (NumberFormatException e) {
				throw new SQLException(String.format("Unable to convert object '%s' of class '%s' to a valid number (row# %d)", value.toString(), value.getClass().getName(), rowNum), e);
			}
		}
	}


	private java.sql.Timestamp toSqlTimestamp(Object value, int rowNum) throws SQLException {
		if (value instanceof java.sql.Timestamp) {
			return (java.sql.Timestamp) value;
		} else if (value instanceof String) {
			return new java.sql.Timestamp(parseStringDate((String) value).getTimeInMillis());
		} else if (value instanceof Number) {
			return new java.sql.Timestamp(((Number) value).longValue());
		} else if (value instanceof java.util.Date) {
			return new java.sql.Timestamp(((java.util.Date) value).getTime());
		} else {
			throw new SQLException(String.format("Unable to convert object '%s' of class '%s' to a valid Timestamp (row# %d)", value.toString(), value.getClass().getName(), rowNum));
		}
	}

	private java.sql.Date toSqlDate(Object value, int rowNum) throws SQLException {
		if (value instanceof java.sql.Date) {
			return (java.sql.Date) value;
		} else if (value instanceof String) {
			return new java.sql.Date(parseStringDate((String) value).getTimeInMillis());
		} else if (value instanceof Number) {
			return new java.sql.Date(((Number) value).longValue());
		} else if (value instanceof java.util.Date) {
			return new java.sql.Date(((java.util.Date) value).getTime());
		} else {
			throw new SQLException(String.format("Unable to convert object '%s' of class '%s' to a valid Date (row# %d)", value.toString(), value.getClass().getName(), rowNum));
		}
	}

	private java.sql.Time toSqlTime(Object value, int rowNum) throws SQLException {
		if (value instanceof java.sql.Time) {
			return (java.sql.Time) value;
		} else if (value instanceof String) {
			try {
				return new java.sql.Time(DatatypeConverter.parseTime(value.toString()).getTimeInMillis());
			} catch (IllegalArgumentException e) {
				throw new SQLException(String.format("Unable to convert string '%s' to a valid Time", value.toString()));
			}
		} else if (value instanceof Number) {
			return new java.sql.Time(((Number) value).longValue());
		} else if (value instanceof java.util.Date) {
			return new java.sql.Time(((java.util.Date) value).getTime());
		} else {
			throw new SQLException(String.format("Unable to convert object '%s' of class '%s' to a valid Time (row# %d)", value.toString(), value.getClass().getName(), rowNum));
		}
	}
	
	/*
	static public void main(String[] argc) {
		Long now = System.currentTimeMillis();
		String s1 = Utils.printIsoDateTime(now);
		System.out.print(String.format("s1:%s    s2:%s\n", s1, s2));
	}*/

	public static Calendar parseStringDate(String date) throws SQLException {
		try {
			if (date.length() > 13) {
				return DatatypeConverter.parseDateTime(date);
			} else {
				return DatatypeConverter.parseDate(date);
			}
		} catch (IllegalArgumentException e) {
			throw new SQLException(String.format("Unable to convert string '%s' to a valid Date", date));
		}
	}
	
	private static Object getTypedValueFromResultSet(ResultSet resultSet, int idx, ResultSetMetaData rsmd) throws SQLException {
		Object o;
		int jdbcType = rsmd.getColumnType(idx);
		switch (jdbcType) {
			case Types.VARCHAR:
			case Types.CHAR:
				String s = resultSet.getString(idx);
				o = (s != null) ? s.trim() : s;
			break;
			case Types.NUMERIC:
				o = resultSet.getBigDecimal(idx);
			break;
			case Types.BIT:
				o = resultSet.getBoolean(idx);
			break;
			case Types.INTEGER:
			case Types.TINYINT:
				o = resultSet.getInt(idx);
			break;
			case Types.BIGINT:
				o = resultSet.getLong(idx);
			break;
			case Types.REAL:
				o = resultSet.getFloat(idx);
			break;
			case Types.FLOAT:
				o = resultSet.getDouble(idx);
			break;
			case Types.BINARY:
				o = resultSet.getBytes(idx);
			break;
			case Types.DATE:
				o = resultSet.getDate(idx);
			break;
			case Types.TIME:
				o = resultSet.getTime(idx);
			break;
			case Types.TIMESTAMP:
				o = resultSet.getTimestamp(idx);
			break;
			case Types.CLOB:
				o = resultSet.getClob(idx);
			break;
			case Types.BLOB:
				o = resultSet.getBlob(idx);
			break;
			case Types.ARRAY:
				o = resultSet.getArray(idx);
			break;
			case Types.REF:
				o = resultSet.getRef(idx);
			break;
			case Types.OTHER:
				String typeName = rsmd.getColumnTypeName(idx);
				log.debug(String.format("Non standard type '%s' on column %d", typeName, idx));
				o = resultSet.getObject(idx);
			break;
			default:
				throw new RuntimeException(String.format("Unhandled jdbcType: %d ", jdbcType));
				// break;
		}
		if (resultSet.wasNull()) {
			o = null; // May be 0 in numeric primitive type
		}
		return o;
	}

	
	
}
