package com.kappaware.k2jdbc.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DbEngine {
	
	void write(String tableName, List<Map<String, Object>> items) throws SQLException, IOException;
	
	List<Map<String, Object>> query(String query, Object[] params) throws SQLException;
	
	int execute(String sql) throws SQLException;
	
	DbCatalog getDbCatalog();
	
	void close();

}
