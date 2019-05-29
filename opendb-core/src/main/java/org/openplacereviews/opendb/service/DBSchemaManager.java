package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataColumnSpec;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
@ConfigurationProperties(prefix = "opendb.db-schema", ignoreInvalidFields = false, ignoreUnknownFields = true)
public class DBSchemaManager {

	protected static final Log LOGGER = LogFactory.getLog(DBSchemaManager.class);
	private static final int OPENDB_SCHEMA_VERSION = 1;
	
	// //////////SYSTEM TABLES DDL ////////////
	protected static String SETTINGS_TABLE = "opendb_settings";
	protected static String BLOCKS_TABLE = "blocks";
	protected static String OPERATIONS_TABLE = "operations";
	protected static String OP_DELETED_TABLE = "op_deleted";
	protected static String OBJS_TABLE = "objs";
	protected static String OPERATIONS_TRASH_TABLE = "operations_trash";
	protected static String BLOCKS_TRASH_TABLE = "blocks_trash";
	protected static String IMAGE_TABLE = "image";

	private static Map<String, List<ColumnDef>> schema = new HashMap<String, List<ColumnDef>>();
	private static final int MAX_KEY_SIZE = 5;

	// loaded from config
	private TreeMap<String, Map<String, Object>> objtables = new TreeMap<String, Map<String, Object>>();
	private TreeMap<String, ObjectTypeTable> objTableDefs = new TreeMap<String, ObjectTypeTable>();
	private TreeMap<String, String> typeToTables = new TreeMap<String, String>();
	

	@Autowired
	private JsonFormatter formatter;

	static class ObjectTypeTable {
		public ObjectTypeTable(String tableName, int keySize) {
			this.tableName = tableName;
			this.keySize = keySize;
		}
		String tableName;
		int keySize;
		Set<String> types = new TreeSet<>();
	}
	
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

		registerColumn(IMAGE_TABLE, "hash", "text PRIMARY KEY", true);
		registerColumn(IMAGE_TABLE, "extension", "text", false);
		registerColumn(IMAGE_TABLE, "cid", "text", false);
		registerColumn(IMAGE_TABLE, "active", "bool", false);
		registerColumn(IMAGE_TABLE, "added", "timestamp", false);

		registerObjTable(OBJS_TABLE, MAX_KEY_SIZE);

	}

	private static void registerObjTable(String tbName, int maxKeySize) {
		registerColumn(tbName, "type", "text", true);
		for (int i = 1; i <= maxKeySize; i++) {
			registerColumn(tbName, "p" + i, "text", true);
		}
		registerColumn(tbName, "ophash", "bytea", true);
		registerColumn(tbName, "superblock", "bytea", true);
		registerColumn(tbName, "sblockid", "int", true);
		registerColumn(tbName, "sorder", "int", true);
		registerColumn(tbName, "content", "jsonb", false);
	}

	public TreeMap<String, Map<String, Object>> getObjtables() {
		return objtables;
	}
	
	public void setObjtables(TreeMap<String, Map<String, Object>> objtables) {
		this.objtables = objtables;
	}

	public Collection<String> getObjectTables() {
		return objTableDefs.keySet();
	}
	
	public String getTableByType(String type) {
		String tableName = typeToTables.get(type);
		if(tableName != null) {
			return tableName;
		}
		return OBJS_TABLE;
	}
	
	public int getKeySizeByType(String type) {
		return getKeySizeByTable(getTableByType(type));
	}
	
	public int getKeySizeByTable(String table) {
		return objTableDefs.get(table).keySize;
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
		
		prepareObjTableMapping();
		migrateDBSchema(jdbcTemplate);
		for (String tableName : schema.keySet()) {
			if(tableName.equals(SETTINGS_TABLE))  {
				 continue;
			}
			List<ColumnDef> cls = schema.get(tableName);
			createTable(metadataDB, jdbcTemplate, tableName, cls);
		}
		
		migrateObjMappingIfNeeded(jdbcTemplate);
	}

	@SuppressWarnings("unchecked")
	private void migrateObjMappingIfNeeded(JdbcTemplate jdbcTemplate) {
		String objMapping = getSetting(jdbcTemplate, "opendb.mapping");
		String newMapping = formatter.toJsonElement(objtables).toString();
		if (!OUtils.equals(newMapping, objMapping)) {
			LOGGER.info(String.format("Start object mapping migration from '%s' to '%s'", objMapping, newMapping));
			TreeMap<String, String> previousTypeToTable = new TreeMap<>();
			if (objMapping != null && objMapping.length() > 0) {
				TreeMap<String, Object> previousMapping = formatter.fromJsonToTreeMap(objMapping);
				for(String tableName : previousMapping.keySet()) {
					Map<String, String> otypes = ((Map<String, Map<String, String>>) previousMapping.get(tableName)).get("types");
					if(otypes != null) {
						for(String type : otypes.values()) {
							previousTypeToTable.put(type, tableName);
						}
					}
				}
			}

			for (String tableName : objTableDefs.keySet()) {
				ObjectTypeTable ott = objTableDefs.get(tableName);
				for (String type : ott.types) {
					String prevTable = previousTypeToTable.remove(type);
					if (prevTable == null) {
						prevTable = OBJS_TABLE;
					}
					migrateObjDataBetweenTables(type, tableName, prevTable, ott.keySize, jdbcTemplate);
				}
			}
			for (String type : previousTypeToTable.keySet()) {
				String prevTable = previousTypeToTable.get(type);
				migrateObjDataBetweenTables(type, OBJS_TABLE, prevTable, MAX_KEY_SIZE, jdbcTemplate);
			}
			setSetting(jdbcTemplate, "opendb.mapping", newMapping);
		}
				
	}

	private void migrateObjDataBetweenTables(String type, String tableName, String prevTable, int keySize, JdbcTemplate jdbcTemplate) {
		if(!OUtils.equals(tableName, prevTable)) {
			LOGGER.info(String.format("Migrate objects of type '%s' from '%s' to '%s'...", type, prevTable, tableName));
			// compare existing table
			String pks = "";
			for(int i = 1; i <= keySize; i++) {
				pks += ", p" +i; 
			}

			List<CustomIndex> valuesForUpdating = jdbcTemplate.query("SELECT ENCODE(ophash, 'hex') as ophash, content FROM " + prevTable + " WHERE type = ?", rs -> {
				List<CustomIndex> indexList = new ArrayList<>();
				while (rs.next()) {
					indexList.add(CustomIndex.of(rs.getString(1), rs.getString(2)));
				}
				return indexList;
			}, type);

			int update = jdbcTemplate.update(
					"WITH moved_rows AS ( DELETE FROM " + prevTable + " a WHERE type = ? RETURNING a.*) " +
					"INSERT INTO " + tableName + "(type, ophash, superblock, sblockid, sorder, content " + pks + ") " +
					"SELECT type, ophash, superblock, sblockid, sorder, content" + pks + " FROM moved_rows", type);

			List<CustomIndex> customIndexList = generateCustomColumnsForTable(tableName);

			StringBuilder columnsForUpdating = new StringBuilder();
			for (CustomIndex c : customIndexList) {
				if (columnsForUpdating.length() == 0) {
					columnsForUpdating.append(c.getColumn()).append(" = ?");
				} else {
					columnsForUpdating.append(",").append(c.getColumn()).append(" = ?");
				}
			}

			if (valuesForUpdating != null) {
				valuesForUpdating.forEach(customIndex -> {
					OpObject opObject = formatter.parseObject(customIndex.getContent());
					Object[] args = new Object[customIndexList.size() + 1];
					AtomicInteger i = new AtomicInteger();

					customIndexList.forEach(indexField -> {
						args[i.get()] = getColumnValue(indexField, opObject);
						i.getAndIncrement();
					});
					args[i.get()] = SecUtils.getHashBytes(customIndex.getHash());

					jdbcTemplate.update(
							"UPDATE " + tableName + " SET " + columnsForUpdating + " WHERE ophash = ? ", args);
				});
			}

			LOGGER.info(String.format("Migrate %d objects of type '%s'.", update, type));
		}
	}

	@SuppressWarnings("unchecked")
	private void prepareObjTableMapping() {
		for(String tableName : objtables.keySet()) {
			Integer i = (Integer) objtables.get(tableName).get("keysize");
			if(i == null) {
				i = MAX_KEY_SIZE;
			}
			ObjectTypeTable ott = new ObjectTypeTable(tableName, i);
			objTableDefs.put(tableName, ott);
			registerObjTable(tableName, i);
			Map<String, String> tps = (Map<String, String>) objtables.get(tableName).get("types");
			if(tps != null) {
				for(String type : tps.values()) {
					typeToTables.put(type, tableName);
					ott.types.add(type);
				}
			}

			LinkedHashMap<String, LinkedHashMap<String, String>> cii = (LinkedHashMap<String, LinkedHashMap<String, String>>) objtables.get(tableName).get("columns");
			if (cii != null) {
				for (Map.Entry<String, LinkedHashMap<String, String>> entry : cii.entrySet()) {
					LinkedHashMap<String, String> arrayColumns = entry.getValue();

					registerColumn(tableName, arrayColumns.get("name"), arrayColumns.get("type"), false);
				}
			}
		}
		objTableDefs.put(OBJS_TABLE, new ObjectTypeTable(OBJS_TABLE, MAX_KEY_SIZE));
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
		return jdbcTemplate.update("insert into  " + SETTINGS_TABLE + "(key,value) values (?, ?) "
				+ " ON CONFLICT (key) DO UPDATE SET value = ? ", key, v, v) != 0;
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

		List<CustomIndex> customIndexList = generateCustomColumnsForTable(table);
		Object[] args = new Object[6 + ksize + customIndexList.size()];
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
		AtomicInteger i = new AtomicInteger(6);

		String columns = generateColumnNames(customIndexList);
		AtomicReference<String> amountValuesForCustomColumns = new AtomicReference<>("");
		customIndexList.forEach(customIndex -> {
			args[i.get()] = getColumnValue(customIndex, obj);
			amountValuesForCustomColumns.set(amountValuesForCustomColumns.get() + "?,");
			i.getAndIncrement();
		});

		pkey.toArray(args, i.get());

		jdbcTemplate.update("INSERT INTO " + table
				+ "(type,ophash,superblock,sblockid,sorder,content," + columns + generatePKString(table, "p%1$d", ",") + ") "
				+ " values(?,?,?,?,?,?," + amountValuesForCustomColumns + generatePKString(table, "?", ",")+ ")", args);
	}

	private Object getColumnValue(CustomIndex customIndex, OpObject obj) {
		switch (customIndex.getType()) {
			case "jsonb" : {
				try {
					PGobject content = new PGobject();
					content.setType(customIndex.getType());
					content.setValue(formatter.fullObjectToJson(obj.getObjectValue(customIndex.getField())));
					return content;
				} catch (Exception e) {
					return null;
				}
			}
			case "text" : {
				try {
					return obj.getStringValue(customIndex.getField());
				} catch (Exception e) {
					return null;
				}
			}
			case "bytea" : {
				try {
					return SecUtils.getHashBytes(obj.getStringValue(customIndex.getField()));
				} catch (Exception e) {
					return null;
				}
			}
			case "integer" : {
				try {
					return obj.getIntValue(customIndex.getField(), 0);
				} catch (Exception e) {
					return null;
				}
			}
			case "bigint" : {
				try {
					return obj.getLongValue(customIndex.getField(), 0);
				} catch (Exception e) {
					return null;
				}
			}
			default: {
				return null;
			}
		}
	}

	public static class CustomIndex {
		private String column;
		private String field;
		private String type;
		private String hash;
		private String content;

		private CustomIndex () {

		}

		public static CustomIndex of(String column, String field, String type) {
			CustomIndex customIndex = new CustomIndex();
			customIndex.column = column;
			customIndex.field = field;
			customIndex.type = type;

			return customIndex;
		}

		public static CustomIndex of(String ophash, String content) {
			CustomIndex customIndex = new CustomIndex();
			customIndex.hash = ophash;
			customIndex.content = content;

			return customIndex;
		}

		public String getHash() {
			return hash;
		}

		public void setHash(String hash) {
			this.hash = hash;
		}

		public String getContent() {
			return content;
		}

		public void setContent(String content) {
			this.content = content;
		}

		public String getColumn() {
			return column;
		}

		public void setColumn(String column) {
			this.column = column;
		}

		public String getField() {
			return field;
		}

		public void setField(String field) {
			this.field = field;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}
	}

	private List<CustomIndex> generateCustomColumnsForTable(String table) {
		List<CustomIndex> columns = new ArrayList<>();

		if (table.equals(OBJS_TABLE)) {
			return columns;
		}

		LinkedHashMap<String, LinkedHashMap<String, String>> cii = (LinkedHashMap<String, LinkedHashMap<String, String>>) objtables.get(table).get("columns");
		if (cii != null) {
			for (Map.Entry<String, LinkedHashMap<String, String>> entry : cii.entrySet()) {
				LinkedHashMap<String, String> arrayColumns = entry.getValue();

				columns.add(CustomIndex.of(arrayColumns.get("name"), arrayColumns.get("field"), arrayColumns.get("type")));
			}
		}

		return columns;
	}

	private String generateColumnNames(List<CustomIndex> columns) {
		AtomicReference<String> customColumns = new AtomicReference<>("");
		columns.forEach(column -> {
			customColumns.set(customColumns.get() + column.getColumn() + ", ");
		});

		return customColumns.get();
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
