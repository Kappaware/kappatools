package com.kappaware.k2cassandra.cassandra;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class DbEngineImpl implements DbEngine {
	private Session session;
	private Map<String, DbTable> tableByName;

	public DbEngineImpl(Cluster cluster) {
		this.session = cluster.connect();
		this.tableByName = new HashMap<String, DbTable>();
	}

	@Override
	public void close() {
		this.session.close();
	}

	@Override
	public DbTable getTable(String name) throws DbEngineException {
		DbTable dbTable = this.tableByName.get(name);
		if (dbTable == null) {
			String[] namea = name.split("\\.");
			if (namea.length != 2) {
				throw new DbEngineException(String.format("'%s' is not a qualified table name. It must be in the form <keySpace>.<tableName>", name));
			}
			KeyspaceMetadata ksmd = session.getCluster().getMetadata().getKeyspace(namea[0]);
			if (ksmd == null) {
				throw new DbEngineException(String.format("Keyspace '%s' is not existing", namea[0]));
			}
			TableMetadata tmd = ksmd.getTable(namea[1]);
			if (tmd == null) {
				throw new DbEngineException(String.format("Table '%s' is not existing in Keyspace '%s'", namea[1], namea[0]));
			}
			dbTable = new DbTableImpl(name, session, tmd);
			this.tableByName.put(name, dbTable);
		}
		return dbTable;
	}

}
