package org.openplacereviews.opendb.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openplacereviews.opendb.OpenDBServer.MetadataColumnSpec;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

@Service
public class DBSchemaManager {
	
	// //////////SYSTEM TABLES DDL ////////////
	protected static String SETTINGS_TABLE = "opendb_settings";
	protected static String BLOCKS_TABLE = "blocks";
	protected static String OPERATIONS_TABLE = "operations";
	protected static String OP_DELETED_TABLE = "op_deleted";
	protected static String OBJS_TABLE = "objs";
	protected static String OPERATIONS_TRASH_TABLE = "operations_trash";
	protected static String BLOCKS_TRASH_TABLE = "blocks_trash";

	private static Map<String, List<ColumnDef>> schema = new HashMap<String, List<ColumnDef>>();
	private static final int MAX_KEY_SIZE = 5;
	
	private static int OPENDB_SCHEMA_VERSION = 1;
	
	@Value("${opendb.db-schema.version}")
	private static int userSchemaVersion = 1;

	@Autowired
	private JsonFormatter formatter;

	// SCHEMA DEFINITION
	private static class ColumnDef {
		String tableName;
		String colName;
		String colType;
		boolean index;
	}

	private static void registerColumn(String tableName, String colName, String colType, boolean index) {
		List<ColumnDef> lst = schema.get(tableName);
		if (lst == null) {
			lst = new ArrayList<ColumnDef>();
			schema.put(tableName, lst);
		}
		ColumnDef cd = new ColumnDef();
		cd.tableName = tableName;
		cd.colName = colName;
		cd.colType = colType;
		cd.index = index;
		lst.add(cd);
	}

	static {
		registerColumn(SETTINGS_TABLE, "key", "text PRIMARY KEY", true);
		registerColumn(SETTINGS_TABLE, "value", "text", false);
		registerColumn(SETTINGS_TABLE, "content", "jsonb", false);
		
		registerColumn(BLOCKS_TABLE, "hash", "bytea PRIMARY KEY", true);
		registerColumn(BLOCKS_TABLE, "phash", "bytea", false);
		registerColumn(BLOCKS_TABLE, "blockid", "int", true);
		registerColumn(BLOCKS_TABLE, "superblock", "bytea", true);
		registerColumn(BLOCKS_TABLE, "header", "jsonb", false);
		registerColumn(BLOCKS_TABLE, "content", "jsonb", false);

		registerColumn(BLOCKS_TRASH_TABLE, "hash", "bytea PRIMARY KEY", true);
		registerColumn(BLOCKS_TRASH_TABLE, "phash", "bytea", false);
		registerColumn(BLOCKS_TRASH_TABLE, "blockid", "int", true);
		registerColumn(BLOCKS_TRASH_TABLE, "time", "timestamp", false);
		registerColumn(BLOCKS_TRASH_TABLE, "content", "jsonb", false);

		registerColumn(OPERATIONS_TABLE, "dbid", "serial not null", false);
		registerColumn(OPERATIONS_TABLE, "hash", "bytea PRIMARY KEY", true);
		registerColumn(OPERATIONS_TABLE, "superblock", "bytea", true);
		registerColumn(OPERATIONS_TABLE, "sblockid", "int", true);
		registerColumn(OPERATIONS_TABLE, "sorder", "int", true);
		registerColumn(OPERATIONS_TABLE, "blocks", "bytea[]", false);
		registerColumn(OPERATIONS_TABLE, "content", "jsonb", false);

		registerColumn(OPERATIONS_TRASH_TABLE, "id", "int", true);
		registerColumn(OPERATIONS_TRASH_TABLE, "hash", "bytea", true);
		registerColumn(OPERATIONS_TRASH_TABLE, "time", "timestamp", false);
		registerColumn(OPERATIONS_TRASH_TABLE, "content", "jsonb", false);

		registerColumn(OP_DELETED_TABLE, "hash", "bytea", true);
		registerColumn(OP_DELETED_TABLE, "superblock", "bytea", true);
		registerColumn(OP_DELETED_TABLE, "shash", "bytea[]", false);
		registerColumn(OP_DELETED_TABLE, "mask", "bigint", false);

		registerColumn(OBJS_TABLE, "type", "text", true);
		registerColumn(OBJS_TABLE, "p1", "text", true);
		registerColumn(OBJS_TABLE, "p2", "text", true);
		registerColumn(OBJS_TABLE, "p3", "text", true);
		registerColumn(OBJS_TABLE, "p4", "text", true);
		registerColumn(OBJS_TABLE, "p5", "text", true);
		registerColumn(OBJS_TABLE, "ophash", "bytea", true);
		registerColumn(OBJS_TABLE, "superblock", "bytea", true);
		registerColumn(OBJS_TABLE, "sblockid", "int", true);
		registerColumn(OBJS_TABLE, "sorder", "int", true);
		registerColumn(OBJS_TABLE, "content", "jsonb", false);

	}

	public List<String> getObjectTables() {
		return Collections.singletonList(OBJS_TABLE);
	}
	
	public String getTableByType(String type) {
		return OBJS_TABLE;
	}
	
	public int getKeySizeByType(String type) {
		return getKeySizeByTable(getTableByType(type));
	}
	
	public int getKeySizeByTable(String table) {
		return MAX_KEY_SIZE;
	}

	public String generatePKString(String objTable, String mainString, String sep) {
		return generatePKString(objTable, mainString, sep, getKeySizeByTable(objTable));
	}
	
	public String generatePKString(String objTable, String mainString, String sep, int ks) {
		String s = "";
		for(int k = 1; k <= ks; k++) {
			if(k > 1) {
				s += sep;
			}
			s += String.format(mainString, k); 
		}
		return s;
	}

	private void migrateDBSchema(JdbcTemplate jdbcTemplate) {
		int dbVersion = getIntSetting(jdbcTemplate, "opendb.version");
		if(dbVersion < OPENDB_SCHEMA_VERSION) {
			setSetting(jdbcTemplate, "opendb.version", OPENDB_SCHEMA_VERSION + "");
		} else if(dbVersion > OPENDB_SCHEMA_VERSION) {
			throw new UnsupportedOperationException();
		}
	}
	
	public void initializeDatabaseSchema(MetadataDb metadataDB, JdbcTemplate jdbcTemplate) {
		createTable(metadataDB, jdbcTemplate, SETTINGS_TABLE, schema.get(SETTINGS_TABLE));
		
		migrateDBSchema(jdbcTemplate);
		
		for (String tableName : schema.keySet()) {
			if(tableName.equals(SETTINGS_TABLE))  {
				 continue;
			}
			List<ColumnDef> cls = schema.get(tableName);
			createTable(metadataDB, jdbcTemplate, tableName, cls);
		}
	}

	private void createTable(MetadataDb metadataDB, JdbcTemplate jdbcTemplate, String tableName, List<ColumnDef> cls) {
		List<MetadataColumnSpec> list = metadataDB.tablesSpec.get(tableName);
		if (list == null) {
			StringBuilder clb = new StringBuilder();
			List<String> indx = new ArrayList<String>();
			for (ColumnDef c : cls) {
				if (clb.length() > 0) {
					clb.append(", ");
				}
				clb.append(c.colName).append(" ").append(c.colType);
				if (c.index) {
					indx.add(String.format("create index %s_%s_ind on %s (%s);\n", c.tableName, c.colName,
							c.tableName, c.colName));
				}
			}
			String createTable = String.format("create table %s (%s)", tableName, clb.toString());
			jdbcTemplate.execute(createTable);
			for (String ind : indx) {
				jdbcTemplate.execute(ind);
			}
		} else {
			for (ColumnDef c : cls) {
				boolean found = false;
				for (MetadataColumnSpec m : list) {
					if (c.colName.equals(m.columnName)) {
						found = true;
						break;
					}
				}
				if (!found) {
					throw new UnsupportedOperationException(String.format("Missing column '%s' in table '%s' ",
							c.colName, tableName));
				}
			}
		}
	}

	private boolean setSetting(JdbcTemplate jdbcTemplate, String key, String v) {
		return jdbcTemplate.update("insert into  " + SETTINGS_TABLE + "(key,value) values (?, ?)", key, v) != 0;
	}
	
	private int getIntSetting(JdbcTemplate jdbcTemplate, String key) {
		String s = getSetting(jdbcTemplate, key);
		if(s == null) {
			return 0;
		}
		return Integer.parseInt(s);
	}
	
	private String getSetting(JdbcTemplate jdbcTemplate, String key) {
		String s = null;
		try {
			s = jdbcTemplate.query("select value from " + SETTINGS_TABLE + " where key = ?", new ResultSetExtractor<String>() {

				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {
					boolean next = rs.next();
					if (next) {
						return rs.getString(1);
					}
					return null;
				}
			}, key);
		} catch (DataAccessException e) {
		}
		return s;
	}

	
	
	public void insertObjIntoTable(String type, CompoundKey pkey, OpObject obj, byte[] superBlockHash, 
			int sblockid, int sorder, JdbcTemplate jdbcTemplate) {
		String table = getTableByType(type);
		int ksize = getKeySizeByType(type);
		if (pkey.size() > ksize) {
			throw new UnsupportedOperationException("Key is too long to be stored: " + pkey.toString());
		}
		
		Object[] args = new Object[6 + ksize];
		args[0] = type;
		String ophash = obj.getParentHash();
		args[1] = SecUtils.getHashBytes(ophash);
		args[2] = superBlockHash;
		
		args[3] = sblockid;
		args[4] = sorder;
		PGobject contentObj = new PGobject();
		contentObj.setType("jsonb");
		try {
			contentObj.setValue(formatter.objToJson(obj));
		} catch (SQLException es) {
			throw new IllegalArgumentException(es);
		}
		args[5] = contentObj;
		pkey.toArray(args, 6);
		

		jdbcTemplate.update("INSERT INTO " + table
				+ "(type,ophash,superblock,sblockid,sorder,content," + generatePKString(table, "p%1$d", ",")+") "
				+ " values(?,?,?,?,?,?," + generatePKString(table, "?", ",")+ ")", args);		
	}
	
	
	

	// Query / insert values
	// select encode(b::bytea, 'hex') from test where b like (E'\\x39')::bytea||'%';
	// insert into test(b) values (decode('39556d070fd95f54b554010207d42605a8d0adfbb3b8b8e134df7df0689d78ab', 'hex'));
	// UPDATE blocks SET superblocks = array_remove(superblocks,
	// decode('39556d070fd95f54b554010207d42605a8d0adfbb3b8b8e134df7df0689d78ab', 'hex'));

	public static void main(String[] args) {

		for (String tableName : schema.keySet()) {
			List<ColumnDef> cls = schema.get(tableName);
			StringBuilder clb = new StringBuilder();
			StringBuilder indx = new StringBuilder();
			for (ColumnDef c : cls) {
				if (clb.length() > 0) {
					clb.append(", ");
				}
				clb.append(c.colName).append(" ").append(c.colType);
				if (c.index) {
					indx.append(String.format("create index %s_%s_ind on %s (%s);\n", c.tableName, c.colName,
							c.tableName, c.colName));
				}
			}
			System.out.println(String.format("create table %s (%s);", tableName, clb.toString()));
			System.out.println(indx.toString());
		}

	}
	


}
