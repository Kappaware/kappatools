/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.k2jdbc.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbCatalogImpl implements DbCatalog {
	static Logger log = LoggerFactory.getLogger(DbCatalogImpl.class);

	private DataSource dataSource;
	private Map<String, DbTableDefImpl> tableByName = new HashMap<String, DbTableDefImpl>();

	
	public DbCatalogImpl(DataSource dataSource) {
		this.dataSource = dataSource;
	}


	@Override
	public DbTableDef getTableDef(String name) throws DbEngineException  {
		DbTableDefImpl tableDef = this.tableByName.get(name);
		if(tableDef != null) {
			return tableDef;
		} else {
			log.debug(String.format("Will lookup info for table '%s'", name));
			Connection connection = null;
			tableDef = new DbTableDefImpl(name);
			try {
				connection = this.dataSource.getConnection();
				ResultSet rs = connection.getMetaData().getColumns(null, null, name, null);
				while (rs.next()) {
					DbColumnDefImpl column = new DbColumnDefImpl();
					column.name = rs.getString("COLUMN_NAME");
					column.position = rs.getInt("ORDINAL_POSITION");
					column.jdbcType = rs.getInt("DATA_TYPE");
					column.length = rs.getInt("COLUMN_SIZE");
					column.nullable = rs.getInt("NULLABLE") == ResultSetMetaData.columnNullable;
					column.defaultValue = rs.getString("COLUMN_DEF");
					tableDef.columnByName.put(column.name, column);
					log.debug(String.format("Find column'%s' position:%d  type:%d   default:%s", column.name, column.position, column.jdbcType, column.defaultValue));
				}
			} catch(SQLException e) {
				throw new DbEngineException("Exception in getTableDef()", e);
				
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException e) {
					}
				}
			}
			if(tableDef.columnByName.size() > 0) {
				this.tableByName.put(tableDef.name, tableDef);
				return tableDef;
			} else {
				log.debug(String.format("Table '%s' seems not defined", name));
				return null;
			}
		}

			
	}

	
	static class DbTableDefImpl implements DbTableDef {
		String name;
		Map<String, DbColumnDef> columnByName = new HashMap<String, DbColumnDef>();
		
		public DbTableDefImpl(String name) {
			this.name = name;
		}

		@Override
		public String getName() {
			return this.name;
		}

		@Override
		public DbColumnDef getColumnDef(String name) {
			return this.columnByName.get(name);
		}
		
	}
	
	
	static class DbColumnDefImpl implements DbColumnDef {
		private String name;
		private int position;
		private int jdbcType;
		private int length;
		private boolean nullable;
		private String defaultValue;

		@Override
		public String getName() {
			return this.name;
		}

		@Override
		public int getPosition() {
			return this.position;
		}

		@Override
		public int getJdbcType() {
			return this.jdbcType;
		}

		@Override
		public int getLength() {
			return this.length;
		}

		@Override
		public boolean isNullable() {
			return this.nullable;
		}

		@Override
		public String getDefaultValue() {
			return this.defaultValue;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setPosition(int position) {
			this.position = position;
		}

		public void setJdbcType(int jdbcType) {
			this.jdbcType = jdbcType;
		}

		public void setLength(int length) {
			this.length = length;
		}

		public void setNullable(boolean nullable) {
			this.nullable = nullable;
		}

		public void setDefaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
		}

		
	}
}
