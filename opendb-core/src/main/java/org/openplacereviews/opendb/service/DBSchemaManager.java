package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataColumnSpec;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpObject;
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

import static org.openplacereviews.opendb.service.DBSchemaManager.IndexType.*;


@Service
@ConfigurationProperties(prefix = "opendb.db-schema", ignoreInvalidFields = false, ignoreUnknownFields = true)
public class DBSchemaManager {

	protected static final Log LOGGER = LogFactory.getLog(DBSchemaManager.class);
	private static final int OPENDB_SCHEMA_VERSION = 1;
	
	// //////////SYSTEM TABLES DDL ////////////
	protected static String SETTINGS_TABLE = "opendb_settings";
	protected static String BLOCKS_TABLE = "blocks";
	protected static String OPERATIONS_TABLE = "operations";
	protected static String OBJS_TABLE = "objs";
	protected static String OPERATIONS_TRASH_TABLE = "operations_trash";
	protected static String BLOCKS_TRASH_TABLE = "blocks_trash";
	protected static String EXT_RESOURCE_TABLE = "resources";
	protected static String OP_OBJ_HISTORY_TABLE = "op_obj_history";

	private static Map<String, List<ColumnDef>> schema = new HashMap<String, List<ColumnDef>>();
	protected static final int MAX_KEY_SIZE = 5;
	protected static final int USER_KEY_SIZE = 2;
	protected static final int HISTORY_TABLE_SIZE = MAX_KEY_SIZE + USER_KEY_SIZE + 6;

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
	public enum IndexType {
		NOT_INDEXED, INDEXED, GIN, GIST
	}
	private static class ColumnDef {
		String tableName;
		String colName;
		String colType;
		IndexType index;
		String indexedField;
	}

	private static void registerColumn(String tableName, String colName, String colType, IndexType indexType) {
		List<ColumnDef> lst = schema.get(tableName);
		if (lst == null) {
			lst = new ArrayList<ColumnDef>();
			schema.put(tableName, lst);
		}
		ColumnDef cd = new ColumnDef();
		cd.tableName = tableName;
		cd.colName = colName;
		cd.colType = colType;
		cd.index = indexType;
		lst.add(cd);
	}

	private static void registerColumn(String tableName, String colName, String colType, List<CustomIndexDto> customIndices, IndexType basicIndexType) {
		List<ColumnDef> lst = schema.get(tableName);
		if (lst == null) {
			lst = new ArrayList<ColumnDef>();
			schema.put(tableName, lst);
		}
		ColumnDef cd = new ColumnDef();
		cd.tableName = tableName;
		cd.colName = colName;
		cd.colType = colType;
		cd.index = basicIndexType;

		if (customIndices != null) {
			CustomIndexDto indexInfo = getIndexInfoByColumnName(colName, customIndices);
			if (indexInfo != null) {
				cd.index = getIndexTypeByNameColumn(indexInfo, basicIndexType);
				cd.indexedField = indexInfo.field;
			}
		}
		lst.add(cd);
	}

	static {
		registerColumn(SETTINGS_TABLE, "key", "text PRIMARY KEY", INDEXED);
		registerColumn(SETTINGS_TABLE, "value", "text", NOT_INDEXED);
		registerColumn(SETTINGS_TABLE, "content", "jsonb", NOT_INDEXED);

		registerColumn(BLOCKS_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		registerColumn(BLOCKS_TABLE, "phash", "bytea", NOT_INDEXED);
		registerColumn(BLOCKS_TABLE, "blockid", "int", INDEXED);
		registerColumn(BLOCKS_TABLE, "superblock", "bytea", INDEXED);
		registerColumn(BLOCKS_TABLE, "header", "jsonb", NOT_INDEXED);
		registerColumn(BLOCKS_TABLE, "content", "jsonb", NOT_INDEXED);

		registerColumn(BLOCKS_TRASH_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		registerColumn(BLOCKS_TRASH_TABLE, "phash", "bytea", NOT_INDEXED);
		registerColumn(BLOCKS_TRASH_TABLE, "blockid", "int", INDEXED);
		registerColumn(BLOCKS_TRASH_TABLE, "time", "timestamp", NOT_INDEXED);
		registerColumn(BLOCKS_TRASH_TABLE, "content", "jsonb", NOT_INDEXED);

		registerColumn(OPERATIONS_TABLE, "dbid", "serial not null", NOT_INDEXED);
		registerColumn(OPERATIONS_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		registerColumn(OPERATIONS_TABLE, "superblock", "bytea", INDEXED);
		registerColumn(OPERATIONS_TABLE, "sblockid", "int", INDEXED);
		registerColumn(OPERATIONS_TABLE, "sorder", "int", INDEXED);
		registerColumn(OPERATIONS_TABLE, "blocks", "bytea[]", NOT_INDEXED);
		registerColumn(OPERATIONS_TABLE, "content", "jsonb", NOT_INDEXED);

		registerColumn(OP_OBJ_HISTORY_TABLE, "sorder", "serial not null", NOT_INDEXED);
		registerColumn(OP_OBJ_HISTORY_TABLE, "blockhash", "bytea", INDEXED);
		registerColumn(OP_OBJ_HISTORY_TABLE, "ophash", "bytea", INDEXED);
		registerColumn(OP_OBJ_HISTORY_TABLE, "type", "text", INDEXED);
		for (int i = 1; i <= USER_KEY_SIZE; i++) {
			registerColumn(OP_OBJ_HISTORY_TABLE, "u" + i, "text", INDEXED);
		}
		for (int i = 1; i <= MAX_KEY_SIZE; i++) {
			registerColumn(OP_OBJ_HISTORY_TABLE, "p" + i, "text", INDEXED);
		}
		registerColumn(OP_OBJ_HISTORY_TABLE, "time", "timestamp", NOT_INDEXED);
		registerColumn(OP_OBJ_HISTORY_TABLE, "obj", "jsonb", NOT_INDEXED);
		registerColumn(OP_OBJ_HISTORY_TABLE, "status", "int", NOT_INDEXED);

		registerColumn(OPERATIONS_TRASH_TABLE, "id", "int", INDEXED);
		registerColumn(OPERATIONS_TRASH_TABLE, "hash", "bytea", INDEXED);
		registerColumn(OPERATIONS_TRASH_TABLE, "time", "timestamp", NOT_INDEXED);
		registerColumn(OPERATIONS_TRASH_TABLE, "content", "jsonb", NOT_INDEXED);

		registerColumn(EXT_RESOURCE_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		registerColumn(EXT_RESOURCE_TABLE, "extension", "text", NOT_INDEXED);
		registerColumn(EXT_RESOURCE_TABLE, "cid", "text", NOT_INDEXED);
		registerColumn(EXT_RESOURCE_TABLE, "active", "bool", NOT_INDEXED);
		registerColumn(EXT_RESOURCE_TABLE, "added", "timestamp", NOT_INDEXED);

		registerObjTable(OBJS_TABLE, MAX_KEY_SIZE, null);

	}

	private static void registerObjTable(String tbName, int maxKeySize, List<CustomIndexDto> customIndexDtoList) {
		registerColumn(tbName, "type", "text", customIndexDtoList, INDEXED);
		for (int i = 1; i <= maxKeySize; i++) {
			registerColumn(tbName, "p" + i, "text", customIndexDtoList, INDEXED);
		}
		registerColumn(tbName, "ophash", "bytea", customIndexDtoList, INDEXED);
		registerColumn(tbName, "superblock", "bytea", customIndexDtoList, INDEXED);
		registerColumn(tbName, "sblockid", "int", customIndexDtoList, INDEXED);
		registerColumn(tbName, "sorder", "int", customIndexDtoList, INDEXED);
		registerColumn(tbName, "content", "jsonb", customIndexDtoList, NOT_INDEXED);
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

			List<CustomIndexDto> valuesForUpdating = jdbcTemplate.query("SELECT ENCODE(ophash, 'hex') as ophash, content FROM " + prevTable + " WHERE type = ?", rs -> {
				List<CustomIndexDto> indexList = new ArrayList<>();
				while (rs.next()) {
					indexList.add(CustomIndexDto.of(rs.getString(1), rs.getString(2)));
				}
				return indexList;
			}, type);

			int update = jdbcTemplate.update(
					"WITH moved_rows AS ( DELETE FROM " + prevTable + " a WHERE type = ? RETURNING a.*) " +
					"INSERT INTO " + tableName + "(type, ophash, superblock, sblockid, sorder, content " + pks + ") " +
					"SELECT type, ophash, superblock, sblockid, sorder, content" + pks + " FROM moved_rows", type);

			List<CustomIndexDto> customIndexDtoList = generateCustomColumnsForTable(tableName);

			StringBuilder columnsForUpdating = new StringBuilder();
			for (CustomIndexDto c : customIndexDtoList) {
				if (columnsForUpdating.length() == 0) {
					columnsForUpdating.append(c.column).append(" = ?");
				} else {
					columnsForUpdating.append(",").append(c.column).append(" = ?");
				}
			}

			if (valuesForUpdating != null) {
				valuesForUpdating.forEach(customIndexDto -> {
					OpObject opObject = formatter.parseObject(customIndexDto.content);
					Object[] args = new Object[customIndexDtoList.size() + 1];
					AtomicInteger i = new AtomicInteger();

					customIndexDtoList.forEach(indexField -> {
						args[i.get()] = getColumnValue(indexField, opObject);
						i.getAndIncrement();
					});
					args[i.get()] = SecUtils.getHashBytes(customIndexDto.hash);

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

			List<CustomIndexDto> indices = null;
			LinkedHashMap<String, LinkedHashMap<String, String>> indexMap = (LinkedHashMap<String, LinkedHashMap<String, String>>) objtables.get(tableName).get("indices");
			if (indexMap != null) {
				indices = new ArrayList<>();
				for (Map.Entry<String, LinkedHashMap<String, String>> entry : indexMap.entrySet()) {
					LinkedHashMap<String, String> index = entry.getValue();

					indices.add(CustomIndexDto.of(index.get("column"), index.get("field"), index.get("type")));
				}
			}

			ObjectTypeTable ott = new ObjectTypeTable(tableName, i);
			objTableDefs.put(tableName, ott);
			registerObjTable(tableName, i, indices);
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

					registerColumn(tableName, arrayColumns.get("name"), arrayColumns.get("type"), indices, NOT_INDEXED);
				}
			}
		}
		objTableDefs.put(OBJS_TABLE, new ObjectTypeTable(OBJS_TABLE, MAX_KEY_SIZE));
	}

	private static CustomIndexDto getIndexInfoByColumnName(String columnName, List<CustomIndexDto> customIndexDtoList) {
		for (CustomIndexDto indexInfo : customIndexDtoList) {
			if (indexInfo.column.equals(columnName)) {
				return indexInfo;
			}
		}
		return null;
	}

	private static IndexType getIndexTypeByNameColumn(CustomIndexDto customIndexDTO, IndexType basicIndexType) {
		IndexType indexType = basicIndexType;
		switch (customIndexDTO.type) {
			case "GIN": {
				indexType = GIN;
				break;
			}
			case "GIST": {
				indexType = GIST;
				break;
			}
		}
		return indexType;
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
				if(c.index != NOT_INDEXED) {
					indx.add(generateIndexQuery(c));
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

	private String generateIndexQuery(ColumnDef c) {
		if (c.index.equals(INDEXED)) {
			return String.format("create index %s_%s_ind on %s (%s);\n", c.tableName, c.colName,
					c.tableName, c.colName);
		} else if (c.index.equals(GIN)) {
			if (c.indexedField == null || c.indexedField.isEmpty()) {
				return String.format("create index %s_%s_gin_ind on %s using gin (to_tsvector('english', %s));\n", c.tableName, c.colName,
						c.tableName, c.colName);
			} else  {
				return String.format("create index %s_%s_gin_ind on %s using gin ((%s->'%s'));\n", c.tableName, c.colName,
						c.tableName, c.colName, c.indexedField);
			}
		} else if (c.index.equals(GIST)) {
				return String.format("create index %s_%s_gist_ind on %s using gist (tsvector(%s));\n", c.tableName, c.colName,
						c.tableName, c.colName);
		}

		return null;
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

	public Object getColumnValue(CustomIndexDto customIndexDTO, OpObject obj) {
		switch (customIndexDTO.type) {
			case "jsonb" : {
				try {
					PGobject content = new PGobject();
					content.setType("jsonb");
					content.setValue(formatter.fullObjectToJson(obj.getObjectValue(customIndexDTO.field)));
					return content;
				} catch (Exception e) {
					return null;
				}
			}
			case "text" : {
				try {
					return obj.getStringValue(customIndexDTO.field);
				} catch (Exception e) {
					return null;
				}
			}
			case "bytea" : {
				try {
					return SecUtils.getHashBytes(obj.getStringValue(customIndexDTO.field));
				} catch (Exception e) {
					return null;
				}
			}
			case "integer" : {
				try {
					return obj.getIntValue(customIndexDTO.field, 0);
				} catch (Exception e) {
					return null;
				}
			}
			case "bigint" : {
				try {
					return obj.getLongValue(customIndexDTO.field, 0);
				} catch (Exception e) {
					return null;
				}
			}
			default: {
				return null;
			}
		}
	}

	public static class CustomIndexDto {
		public String column;
		private String field;
		private String type;
		private String hash;
		private String content;

		private CustomIndexDto() {

		}

		public static CustomIndexDto of(String column, String field, String type) {
			CustomIndexDto customIndexDTO = new CustomIndexDto();
			customIndexDTO.column = column;
			customIndexDTO.field = field;
			customIndexDTO.type = type;

			return customIndexDTO;
		}

		public static CustomIndexDto of(String ophash, String content) {
			CustomIndexDto customIndexDTO = new CustomIndexDto();
			customIndexDTO.hash = ophash;
			customIndexDTO.content = content;

			return customIndexDTO;
		}
	}

	public List<CustomIndexDto> generateCustomColumnsForTable(String table) {
		List<CustomIndexDto> columns = new ArrayList<>();

		if (table.equals(OBJS_TABLE)) {
			return columns;
		}

		LinkedHashMap<String, LinkedHashMap<String, String>> cii = (LinkedHashMap<String, LinkedHashMap<String, String>>) objtables.get(table).get("columns");
		if (cii != null) {
			for (Map.Entry<String, LinkedHashMap<String, String>> entry : cii.entrySet()) {
				LinkedHashMap<String, String> arrayColumns = entry.getValue();

				columns.add(CustomIndexDto.of(arrayColumns.get("name"), arrayColumns.get("field"), arrayColumns.get("type")));
			}
		}

		return columns;
	}

	public String generateColumnNames(List<CustomIndexDto> columns) {
		AtomicReference<String> customColumns = new AtomicReference<>("");
		columns.forEach(column -> {
			if (customColumns.get().isEmpty()) {
				customColumns.set(customColumns.get() + column.column);
			} else {
				customColumns.set(customColumns.get() + ", " + column.column);
			}
		});

		return customColumns.get();
	}

	public void insertObjIntoTableBatch(List<Object[]> args, String table, JdbcTemplate jdbcTemplate, String columns, AtomicReference<String> amountValuesForCustomColumns) {
		jdbcTemplate.batchUpdate("INSERT INTO " + table
				+ "(type,ophash,superblock,sblockid,sorder,content" + (columns.isEmpty() ? "," : "," + columns + ",") + generatePKString(table, "p%1$d", ",") + ") "
				+ " values(?,?,?,?,?,?," + amountValuesForCustomColumns + generatePKString(table, "?", ",") + ")", args);
	}

	public void insertObjIntoHistoryTableBatch(List<Object[]> args, String table, JdbcTemplate jdbcTemplate) {
		jdbcTemplate.batchUpdate("INSERT INTO " + table + "(blockhash, ophash, type, time, obj, status," +
				generatePKString(table, "u%1$d", ",", USER_KEY_SIZE) + "," +
				generatePKString(table, "p%1$d", ",", MAX_KEY_SIZE) + ") VALUES ("+
				generatePKString(table, "?", ",", HISTORY_TABLE_SIZE) + ")", args);
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
				if (c.index.equals(INDEXED)) {
					indx.append(String.format("create index %s_%s_ind on %s (%s);\n", c.tableName, c.colName,
							c.tableName, c.colName));
				}
			}
			System.out.println(String.format("create table %s (%s);", tableName, clb.toString()));
			System.out.println(indx.toString());
		}

	}
	


}
