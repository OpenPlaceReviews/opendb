package org.openplacereviews.opendb.service;

import static org.openplacereviews.opendb.service.DBSchemaManager.IndexType.GIN;
import static org.openplacereviews.opendb.service.DBSchemaManager.IndexType.GIST;
import static org.openplacereviews.opendb.service.DBSchemaManager.IndexType.INDEXED;
import static org.openplacereviews.opendb.service.DBSchemaManager.IndexType.NOT_INDEXED;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataColumnSpec;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpBlockChain.SearchType;
import org.openplacereviews.opendb.ops.OpIndexColumn;
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
	private TreeMap<String, Map<String, OpIndexColumn>> indexes = new TreeMap<>();
	

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
	
	
	public static class ColumnDef {
		public String tableName;
		public String colName;
		public String colType;
		public IndexType index;
		
		public boolean isArray() {
			return colType.endsWith("[]");
		}
	}

	private static void registerColumn(String tableName, String colName, String colType, IndexType basicIndexType) {
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

		registerObjTable(OBJS_TABLE, MAX_KEY_SIZE);

	}

	private static void registerObjTable(String tbName, int maxKeySize) {
		registerColumn(tbName, "type", "text", INDEXED);
		for (int i = 1; i <= maxKeySize; i++) {
			registerColumn(tbName, "p" + i, "text", INDEXED);
		}
		registerColumn(tbName, "ophash", "bytea", INDEXED);
		registerColumn(tbName, "superblock", "bytea", INDEXED);
		registerColumn(tbName, "sblockid", "int",  INDEXED);
		registerColumn(tbName, "sorder", "int", INDEXED);
		registerColumn(tbName, "content", "jsonb", NOT_INDEXED);
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
		return repeatString(mainString, sep, getKeySizeByTable(objTable));
	}
	
	public String generatePKString(String objTable, String mainString, String sep, int ks) {
		return repeatString(mainString, sep, ks);
	}
	
	public String repeatString(String mainString, String sep, int ks) {
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

			List<OpIndexColumn> valuesForUpdating = jdbcTemplate.query("SELECT ENCODE(ophash, 'hex') as ophash, content FROM " + prevTable + " WHERE type = ?", rs -> {
				List<OpIndexColumn> indexList = new ArrayList<>();
				while (rs.next()) {
					indexList.add(OpIndexColumn.of(rs.getString(1), rs.getString(2)));
				}
				return indexList;
			}, type);

			int update = jdbcTemplate.update(
					"WITH moved_rows AS ( DELETE FROM " + prevTable + " a WHERE type = ? RETURNING a.*) " +
					"INSERT INTO " + tableName + "(type, ophash, superblock, sblockid, sorder, content " + pks + ") " +
					"SELECT type, ophash, superblock, sblockid, sorder, content" + pks + " FROM moved_rows", type);

			List<OpIndexColumn> customIndexDtoList = generateCustomColumnsForTable(tableName);

			StringBuilder columnsForUpdating = new StringBuilder();
			for (OpIndexColumn c : customIndexDtoList) {
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

			Map<String, Map<String, String>> cii = (Map<String, Map<String, String>>) objtables.get(tableName).get("columns");
			if (cii != null) {
				for (Map.Entry<String, Map<String, String>> entry : cii.entrySet()) {
					Map<String, String> arrayColumns = entry.getValue();
					
					registerColumn(tableName, arrayColumns.get("name"), arrayColumns.get("type"), indices, NOT_INDEXED);
				}
			}
			
			List<OpIndexColumn> indices = null;
			Map<String, Map<String, Object>> indexMap = (Map<String, Map<String, Object>>) objtables.get(tableName).get("indices");
			if (indexMap != null) {
				indices = new ArrayList<>();
				for (Map.Entry<String, Map<String, Object>> entry : indexMap.entrySet()) {
					Map<String, Object> index = entry.getValue();
					List<String> indicesList = new ArrayList<>();
					Map<String, String> fieldList = ((Map<String, String>) index.get("field"));
					if (fieldList != null) {
						indicesList.addAll(fieldList.values());
					}
					indices.add(OpIndexColumn.of(index.get("column").toString(), indicesList, index.get("type").toString()));
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
				return String.format("create index %s_%s_gin_ind on %s using gin (%s);\n", c.tableName, c.colName,
						c.tableName, c.colName);
		} else if (c.index.equals(GIST)) {
				return String.format("create index %s_%s_gist_ind on %s using gist (tsvector(%s));\n", c.tableName, c.colName,
						c.tableName, c.colName);
		}

		return null;
	}

	public String generateIndexQuery(OpIndexColumn index, ObjectsSearchRequest req) {
		ColumnDef cd = index.getColumnDef();
		return "SELECT content FROM " + cd.tableName + 
				" WHERE " + getExpressionSignWithColumnName(cd, req.searchType) 
				+ " AND superblock = ?";
	}

	private String getExpressionSignWithColumnName(ColumnDef columnDef, SearchType searchType) {
		if(searchType != SearchType.STRING_EQUALS) {
			throw new UnsupportedOperationException();
		}
		switch (columnDef.index) {
			case INDEXED : {
				return columnDef.colName + " = ?";
			}
			case GIN : {
				return columnDef.colName + " @> ?";
			}
			case GIST : {
				throw new UnsupportedOperationException("GIST index not implemented");
			}
			default: {
				return columnDef.colName + " = ?";
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

	



	@SuppressWarnings("unchecked")
	public List<OpIndexColumn> generateCustomColumnsForTable(String type) {
		List<OpIndexColumn> columns = new ArrayList<>();

		if (table.equals(OBJS_TABLE)) {
			return columns;
		}

		Map<String, Map<String, Object>> cii = (Map<String, Map<String, Object>>) objtables.get(table).get("columns");
		if (cii != null) {
			for (Map.Entry<String, Map<String, Object>> entry : cii.entrySet()) {
				Map<String, Object> arrayColumns = entry.getValue();
				List<String> columnList = new ArrayList<>();
				Map<String, String> fieldList = ((Map<String, String>) arrayColumns.get("field"));
				if (fieldList != null) {
					columnList.addAll(fieldList.values());
				}

				columns.add(OpIndexColumn.of(arrayColumns.get("name").toString(), columnList, arrayColumns.get("type").toString()));
			}
		}

		return columns;
	}
	
	public OpIndexColumn getIndex(String type, String columnId) {
		Map<String, OpIndexColumn> tind = indexes.get(type);
		if(tind != null) {
			return tind.get(columnId);
		}
		return null;
	}
	
	public Collection<OpIndexColumn> getIndicesForType(String type) {
		if(type == null) {
			List<OpIndexColumn> ind = new ArrayList<>();
			Iterator<Map<String, OpIndexColumn>> it = indexes.values().iterator();
			while(it.hasNext()) {
				ind.addAll(it.next().values());
			}
			return ind;
		}
		Map<String, OpIndexColumn> tind = indexes.get(type);
		if(tind != null) {
			return tind.values();
		}
		return Collections.emptyList();
	}

	public void insertObjIntoTableBatch(List<Object[]> args, String table, JdbcTemplate jdbcTemplate, List<OpIndexColumn> indexes) {
		StringBuilder extraColumnNames = new StringBuilder();
		for(OpIndexColumn index : indexes) {
			extraColumnNames.append(index.getColumnDef().colName).append(",");
		}
		jdbcTemplate.batchUpdate("INSERT INTO " + table
				+ "(type,ophash,superblock,sblockid,sorder,content,"
				+ extraColumnNames.toString()
				+ generatePKString(table, "p%1$d", ",") + ") "
				+ " values(?,?,?,?,?,?," + repeatString("?", ",", indexes.size()) + generatePKString(table, "?", ",") + ")", args);
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
