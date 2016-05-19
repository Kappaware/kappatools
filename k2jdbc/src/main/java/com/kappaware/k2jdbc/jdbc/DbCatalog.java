package com.kappaware.k2jdbc.jdbc;

import java.sql.SQLException;

public interface DbCatalog {

	DbTableDef getTableDef(String name) throws SQLException;
	
	public interface DbTableDef {
		String getName();
		DbColumnDef getColumnDef(String name);
	}

	public interface DbColumnDef {
		String getName();
		int getPosition();
		int getJdbcType();
		int getLength();
		boolean isNullable();
		String getDefaultValue();
	}
}
