package org.openplacereviews.opendb.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.de.ColumnDef;
import org.openplacereviews.opendb.ops.de.ColumnDef.IndexType;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.service.SettingsManager.MapStringObjectPreference;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpBlockChain.OP_CHANGE_DELETE;
import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.INDEXED;
import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.NOT_INDEXED;

@Service
public class DBSchemaManager {

	protected static final Log LOGGER = LogFactory.getLog(DBSchemaManager.class);
	private static final int OPENDB_SCHEMA_VERSION = 7;
	
	// //////////SYSTEM TABLES DDL ////////////
	protected static final String SETTINGS_TABLE = "opendb_settings";
	protected static final String BLOCKS_TABLE = "blocks";
	protected static final String OPERATIONS_TABLE = "operations";
	protected static final String OBJS_TABLE = "objs";
	protected static final String OPERATIONS_TRASH_TABLE = "operations_trash";
	protected static final String BLOCKS_TRASH_TABLE = "blocks_trash";
	protected static final String EXT_RESOURCE_TABLE = "resources";
	protected static final String OP_OBJ_HISTORY_TABLE = "op_obj_history";

	private static DBSchemaHelper dbschema = new DBSchemaHelper(SETTINGS_TABLE);
	protected static final int MAX_KEY_SIZE = 5;
	public static final String[] INDEX_P = new String[MAX_KEY_SIZE];
	{
		for(int i = 0; i < MAX_KEY_SIZE; i++) {
			INDEX_P[i] = "p" + (i + 1);
		}
	}
	protected static final int HISTORY_USERS_SIZE = 2;
	private static final int BATCH_SIZE = 1000;

	// loaded from config
	private Map<String, Map<String, Object>> objtables;
	private TreeMap<String, ObjectTypeTable> objTableDefs = new TreeMap<String, ObjectTypeTable>();
	private TreeMap<String, String> typeToTables = new TreeMap<String, String>();
	private TreeMap<String, Map<String, OpIndexColumn>> indexes = new TreeMap<>();

	@Autowired
	private IPFSFileManager externalResourceService;

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private SettingsManager settingsManager;

	static class ObjectTypeTable {
		public ObjectTypeTable(String tableName, int keySize) {
			this.tableName = tableName;
			this.keySize = keySize;
		}
		String tableName;
		int keySize;
		Set<String> types = new TreeSet<>();
	}
	
	public Map<String, Map<String, Object>> getIndexState(boolean actualState) {
		LinkedHashMap<String, Map<String, Object>> state = new LinkedHashMap<String, Map<String,Object>>();
		List<CommonPreference<Map<String, Object>>> userIndexes = settingsManager.getPreferencesByPrefix(
				actualState? SettingsManager.DB_SCHEMA_INTERNAL_INDEXES : SettingsManager.DB_SCHEMA_INDEXES);
		for(CommonPreference<Map<String, Object>> p : userIndexes) {
			state.put(p.getId(), p.get());
		}
		return state;
	}

	public Map<String, Map<String, OpIndexColumn>> getIndexes() {
		return indexes;
	}
	


	static {

		dbschema.registerColumn(BLOCKS_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "phash", "bytea", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "blockid", "int", INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "superblock", "bytea", INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "opcount", "int", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "objdeleted", "int", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "objedited", "int", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "objadded", "int", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "blocksize", "int", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "header", "jsonb", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TABLE, "content", "jsonb", NOT_INDEXED);

		dbschema.registerColumn(BLOCKS_TRASH_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		dbschema.registerColumn(BLOCKS_TRASH_TABLE, "phash", "bytea", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TRASH_TABLE, "blockid", "int", INDEXED);
		dbschema.registerColumn(BLOCKS_TRASH_TABLE, "time", "timestamp", NOT_INDEXED);
		dbschema.registerColumn(BLOCKS_TRASH_TABLE, "content", "jsonb", NOT_INDEXED);

		dbschema.registerColumn(OPERATIONS_TABLE, "dbid", "serial not null", NOT_INDEXED);
		dbschema.registerColumn(OPERATIONS_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		dbschema.registerColumn(OPERATIONS_TABLE, "type", "text", INDEXED);
		dbschema.registerColumn(OPERATIONS_TABLE, "superblock", "bytea", INDEXED);
		dbschema.registerColumn(OPERATIONS_TABLE, "sblockid", "int", INDEXED);
		dbschema.registerColumn(OPERATIONS_TABLE, "sorder", "int", INDEXED);
		dbschema.registerColumn(OPERATIONS_TABLE, "blocks", "bytea[]", NOT_INDEXED);
		dbschema.registerColumn(OPERATIONS_TABLE, "content", "jsonb", NOT_INDEXED);

		dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "sorder", "serial not null", NOT_INDEXED);
		dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "blockhash", "bytea", INDEXED);
		dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "ophash", "bytea", INDEXED);
		dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "type", "text", INDEXED);
		for (int i = 1; i <= HISTORY_USERS_SIZE; i++) {
			dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "usr_" + i, "text", INDEXED);
			dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "login_" + i, "text", INDEXED);
		}
		for (int i = 1; i <= MAX_KEY_SIZE; i++) {
			dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "p" + i, "text", INDEXED);
		}
		dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "time", "timestamp", NOT_INDEXED);
		dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "obj", "jsonb", NOT_INDEXED);
		dbschema.registerColumn(OP_OBJ_HISTORY_TABLE, "status", "int", NOT_INDEXED);

		dbschema.registerColumn(OPERATIONS_TRASH_TABLE, "id", "int", INDEXED);
		dbschema.registerColumn(OPERATIONS_TRASH_TABLE, "hash", "bytea", INDEXED);
		dbschema.registerColumn(OPERATIONS_TRASH_TABLE, "type", "text", INDEXED);
		dbschema.registerColumn(OPERATIONS_TRASH_TABLE, "time", "timestamp", NOT_INDEXED);
		dbschema.registerColumn(OPERATIONS_TRASH_TABLE, "content", "jsonb", NOT_INDEXED);

		dbschema.registerColumn(EXT_RESOURCE_TABLE, "hash", "bytea PRIMARY KEY", INDEXED);
		dbschema.registerColumn(EXT_RESOURCE_TABLE, "extension", "text", NOT_INDEXED);
		dbschema.registerColumn(EXT_RESOURCE_TABLE, "cid", "text", NOT_INDEXED);
		dbschema.registerColumn(EXT_RESOURCE_TABLE, "added", "timestamp", NOT_INDEXED);
		dbschema.registerColumn(EXT_RESOURCE_TABLE, "blocks", "bytea[]", NOT_INDEXED);


		registerObjTable(OBJS_TABLE, MAX_KEY_SIZE);

	}

	private static void registerObjTable(String tbName, int maxKeySize) {
		dbschema.registerColumn(tbName, "type", "text", INDEXED);
		for (int i = 1; i <= maxKeySize; i++) {
			dbschema.registerColumn(tbName, "p" + i, "text", INDEXED);
		}
		dbschema.registerColumn(tbName, "ophash", "bytea", INDEXED);
		dbschema.registerColumn(tbName, "superblock", "bytea", INDEXED);
		dbschema.registerColumn(tbName, "sblockid", "int",  INDEXED);
		dbschema.registerColumn(tbName, "sorder", "int", INDEXED);
		dbschema.registerColumn(tbName, "content", "jsonb", NOT_INDEXED);
	}

	public Map<String, Map<String, Object>> getObjtables() {
		if (objtables != null) {
			return objtables;
		} else {
			Map<String, Map<String, Object>> objtable = new TreeMap<>();
			List<CommonPreference<Map<String, Object>>> preferences = settingsManager.getPreferencesByPrefix(SettingsManager.DB_SCHEMA_OBJTABLES);
			for (CommonPreference<Map<String, Object>> opendbPreference : preferences) {
				MapStringObjectPreference mp = (MapStringObjectPreference) opendbPreference;
				String tableName = mp.getStringValue(SettingsManager.OBJTABLE_TABLENAME, "");
				objtable.put(tableName, mp.get());
			}
			objtables = objtable;
			return objtable;
		}
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

	public List<String> getTypesByTable(String table) {
		List<String> types = new ArrayList<String>();
		for (Map.Entry<String, String> entry : typeToTables.entrySet()) {
			if (table.equals(entry.getValue())) {
				types.add(entry.getKey());
			}
		}
		return types;
	}
	
	// this is confusing method and it is incorrect cause we don't know precise key length for type
//	public int getKeySizeByType(String type) {
//		return getKeySizeByTable(getTableByType(type));
//	}
	
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
		int dbVersion = dbschema.getIntSetting(jdbcTemplate, "opendb.version");
		if (dbVersion < OPENDB_SCHEMA_VERSION) {
			if (dbVersion <= 1) {
				setOperationsType(jdbcTemplate, OPERATIONS_TABLE);
				setOperationsType(jdbcTemplate, OPERATIONS_TRASH_TABLE);
			}
			if (dbVersion <= 3) {
				addBlockAdditionalInfo(jdbcTemplate);
			}
			if (dbVersion <= 4) {
				List<CommonPreference<Map<String, Object>>> prefs = settingsManager.getPreferencesByPrefix(SettingsManager.DB_SCHEMA_INDEXES);
				for(CommonPreference<Map<String, Object>> p : prefs) {
					CommonPreference<Map<String, Object>> ps = settingsManager.registerMapPreferenceForFamily(SettingsManager.DB_SCHEMA_INTERNAL_INDEXES, p.get());
					ps.set(p.get());
				}
			}
			if (dbVersion < 6) {
				updateExternalResources(jdbcTemplate);
			}
			if (dbVersion < 7) {
				setFixMultipleDeletions(jdbcTemplate);
			}
			//setSetting(jdbcTemplate, "opendb.version", OPENDB_SCHEMA_VERSION + "");
		} else if (dbVersion > OPENDB_SCHEMA_VERSION) {
			throw new UnsupportedOperationException();
		}
	}
	
	private void handleBatch(JdbcTemplate jdbcTemplate, List<Object[]> batchArgs, String batchQuery, boolean force) {
		if(batchArgs.size() >= BATCH_SIZE || (force && batchArgs.size() > 0)) {
			jdbcTemplate.batchUpdate(batchQuery, batchArgs);
			batchArgs.clear();
		}
	}

	private void updateExternalResources(JdbcTemplate jdbcTemplate) {
		LOGGER.info("Adding new columns: 'blocks' for table: " + EXT_RESOURCE_TABLE);
		final OpBlockchainRules rules = new OpBlockchainRules(formatter, null);
		jdbcTemplate.query("SELECT content FROM " + BLOCKS_TABLE, new ResultSetExtractor<Integer>() {
			@Override
			public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
				int i = 0;
				while (rs.next()) {
					OpBlock opBlock = formatter.parseBlock(rs.getString(1));
					externalResourceService.processOperations(opBlock);
					i++;
				}
				LOGGER.info("Updated " + i + " blocks");
				return i;
			}
		});
	}

	private void addBlockAdditionalInfo(JdbcTemplate jdbcTemplate) {
		LOGGER.info("Adding new columns: 'opcount, objdeleted, objedited, objadded' for table: " + BLOCKS_TABLE);
		final OpBlockchainRules rules = new OpBlockchainRules(formatter, null);
		jdbcTemplate.query("SELECT content FROM " + BLOCKS_TABLE, new ResultSetExtractor<Integer>() {
			@Override
			public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
				int i = 0;
				while (rs.next()) {
					OpBlock opBlock = formatter.parseBlock(rs.getString(1));
					OpBlock blockheader = OpBlock.createHeader(opBlock, rules);
					PGobject blockHeaderObj = new PGobject();
					blockHeaderObj.setType("jsonb");
					try {
						blockHeaderObj.setValue(formatter.fullObjectToJson(blockheader));
					} catch (SQLException e) {
						throw new IllegalArgumentException(e);
					}
					jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + 
							" set opcount = ?, objdeleted = ?, objedited = ?, objadded = ?, blocksize = ?, header = ? " +
									" WHERE hash = ?",
									blockheader.getIntValue(OpBlock.F_OPERATIONS_SIZE, 0),
									blockheader.getIntValue(OpBlock.F_OBJ_DELETED, 0),
									blockheader.getIntValue(OpBlock.F_OBJ_EDITED, 0),
									blockheader.getIntValue(OpBlock.F_OBJ_ADDED, 0),
									blockheader.getIntValue(OpBlock.F_BLOCK_SIZE, 0),
									blockHeaderObj, 
							SecUtils.getHashBytes(opBlock.getRawHash()));
					i++;
				}
				LOGGER.info("Updated " + i + " blocks");
				return i;
			}
		});
	}
	
	private void setOperationsType(JdbcTemplate jdbcTemplate, String table) {
		LOGGER.info("Indexing operation types required for db version 2: " + table);
		List<Object[]> batchArgs = new ArrayList<Object[]>();
		String batchQuery = "update " + table + " set type = ? where hash = ?";
		jdbcTemplate.query("select hash, content from " + table, new RowCallbackHandler() {

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				OpOperation op = formatter.parseOperation(rs.getString(2));
				Object[] args = new Object[2];
				args[0] = op.getType();
				args[1] = rs.getObject(1);
				batchArgs.add(args);
				handleBatch(jdbcTemplate, batchArgs, batchQuery, false);
			}
		});
		handleBatch(jdbcTemplate, batchArgs, batchQuery, true);
	}

	private void setFixMultipleDeletions(JdbcTemplate jdbcTemplate) {
		LOGGER.info("Fix operations with errorneous multiple deletions");
		List<OpOperation> ops = new ArrayList<>();
		List<OpObject> objs = new ArrayList<>();
		jdbcTemplate.query("SELECT content FROM " + OPERATIONS_TABLE +
				" WHERE content @@ '$.type == \"opr.place\"' " +
				" AND content @@ '$.edit.change.keyvalue().key like_regex \"^images\\\\.([a-zA-Z]*)(\\\\[\\\\d\\\\])$\"'", new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				OpOperation op = formatter.parseOperation(rs.getString(1));
				for (OpObject editObject : op.getEdited()) {
					Map<String, Object> changedMap = editObject.getChangedEditFields();
					int deleteCounter = 0;
					for (Map.Entry<String, Object> e : changedMap.entrySet()) {
						Object opValue = e.getValue();
						if (opValue instanceof Map) {
							continue;
						}
						String opValueStr = opValue.toString();
						if (OP_CHANGE_DELETE.equals(opValueStr)) {
							deleteCounter++;
						}
						if (deleteCounter > 1) {
							ops.add(op);
							objs.add(editObject);
							break;
						}
					}
				}
			}
		});
		LOGGER.info("Fixed " + ops.size() + " operations");
	}

	public void initializeDatabaseSchema(MetadataDb metadataDB, JdbcTemplate jdbcTemplate) {
		dbschema.initSettingsTable(metadataDB, jdbcTemplate);
		prepareObjTableMapping();
		prepareCustomIndices(jdbcTemplate);
		dbschema.createTablesIfNeeded(metadataDB, jdbcTemplate);
		migrateDBSchema(jdbcTemplate);
		migrateObjMappingIfNeeded(jdbcTemplate);
	}

	@SuppressWarnings("unchecked")
	private void migrateObjMappingIfNeeded(JdbcTemplate jdbcTemplate) {
		String objMapping = getSetting(jdbcTemplate, "opendb.mapping");
		String newMapping = formatter.toJsonElement(getObjtables()).toString();
		if (!OUtils.equals(newMapping, objMapping)) {
			LOGGER.info(String.format("Start object mapping migration from '%s' to '%s'", objMapping, newMapping));
			TreeMap<String, String> previousTypeToTable = new TreeMap<>();
			if (objMapping != null && objMapping.length() > 0) {
				TreeMap<String, Object> previousMapping = formatter.fromJsonToTreeMap(objMapping);
				for(String tableName : previousMapping.keySet()) {
					Object types = ((Map<String, Object>) previousMapping.get(tableName)).get("types");
					Collection<String> otypes = null;
					if(types instanceof Collection) {
						otypes = (Collection<String>) types;
					} else if(types instanceof Map) {
						otypes = ((Map<Object, String>) types).values();
					}
					if(otypes != null) {
						for(String type : otypes) {
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
				int keySize = objTableDefs.get(prevTable).keySize;
				migrateObjDataBetweenTables(type, OBJS_TABLE, prevTable, keySize, jdbcTemplate);
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
			int update = jdbcTemplate.update(
					"WITH moved_rows AS ( DELETE FROM " + prevTable + " a WHERE type = ? RETURNING a.*) " +
					"INSERT INTO " + tableName + "(type, ophash, superblock, sblockid, sorder, content " + pks + ") " +
					"SELECT type, ophash, superblock, sblockid, sorder, content" + pks + " FROM moved_rows", type);

			
			LOGGER.info(String.format("Migrate %d objects of type '%s'.", update, type));
		}
	}

	private void prepareCustomIndices(JdbcTemplate jdbcTemplate) {
		List<CommonPreference<Map<String, Object>>> indexes = settingsManager.getPreferencesByPrefix(SettingsManager.DB_SCHEMA_INDEXES);
		for(CommonPreference<Map<String, Object>> pind : indexes) {
			Map<String, Object> indexDef = pind.get();
			generateIndexColumn(indexDef);
		}
	}

	private IndexType getIndexType(String index) {
		if (index != null) {
			if (index.equalsIgnoreCase("true")) {
				return INDEXED;
			} else {
				return ColumnDef.IndexType.valueOf(index);
			}
		}
		return null;
	}

	public ColumnDef generateIndexColumn(Map<String, Object> entry) {
		String name = (String) entry.get(SettingsManager.INDEX_NAME);
		String tableName = (String) entry.get(SettingsManager.INDEX_TABLENAME);
		String colType = (String) entry.get(SettingsManager.INDEX_SQL_TYPE);
		String index = (String) entry.get(SettingsManager.INDEX_INDEX_TYPE);
		Integer cacheRuntime = entry.get(SettingsManager.INDEX_CACHE_RUNTIME_MAX) == null ? null
				: ((Number) entry.get(SettingsManager.INDEX_CACHE_RUNTIME_MAX)).intValue();
		Integer cacheDB = entry.get(SettingsManager.INDEX_CACHE_DB_MAX) == null ? null : ((Number) entry.get(SettingsManager.INDEX_CACHE_DB_MAX)).intValue();
		@SuppressWarnings("unchecked")
		List<String> fld = (List<String>) entry.get(SettingsManager.INDEX_FIELD);
		IndexType di = getIndexType(index);

		ColumnDef cd = new ColumnDef(tableName, name, colType, di);
		// to be used array
		// String sqlmapping = (String) entry.get("sqlmapping");
		if (fld != null) {
			ObjectTypeTable objectTypeTable = objTableDefs.get(tableName);
			for (String type : objectTypeTable.types) {
				OpIndexColumn indexColumn = new OpIndexColumn(type, name, -1, cd);
				if (cacheRuntime != null) {
					indexColumn.setCacheRuntimeBlocks(cacheRuntime);
				}
				if (cacheDB != null) {
					indexColumn.setCacheDBBlocks(cacheDB);
				}
				indexColumn.setFieldsExpression(fld);
				addIndexCol(indexColumn);
			}
		}
		return dbschema.registerColumn(tableName, cd);
	}


	@SuppressWarnings("unchecked")
	private void prepareObjTableMapping() {
		for(String tableName : getObjtables().keySet()) {
			Number i = ((Number) getObjtables().get(tableName).get("keysize"));
			if (i == null) {
				i = MAX_KEY_SIZE;
			}
			registerObjTable(tableName, i.intValue());
			ObjectTypeTable ott = new ObjectTypeTable(tableName, i.intValue());
			objTableDefs.put(tableName, ott);
			List<String> tps = (List<String>) getObjtables().get(tableName).get("types");
			if(tps != null) {
				for(String type : tps) {
					typeToTables.put(type, tableName);
					ott.types.add(type);
					for(ColumnDef c : dbschema.schema.get(tableName)) {
						for(int indId = 0 ; indId < MAX_KEY_SIZE; indId++) {
							if(c.getColName().equals(INDEX_P[indId])) {
								addIndexCol(new OpIndexColumn(type, INDEX_P[indId], indId, c));
							}
						}
					}
				}
			}
		}
		objTableDefs.put(OBJS_TABLE, new ObjectTypeTable(OBJS_TABLE, MAX_KEY_SIZE));
	}

	
	public void removeIndex(JdbcTemplate jdbcTemplate, Map<String, Object> entry) {
		String colName = (String) entry.get(SettingsManager.INDEX_NAME);
		String tableName = (String) entry.get(SettingsManager.INDEX_TABLENAME);
		String index = (String) entry.get(SettingsManager.INDEX_INDEX_TYPE);
		IndexType it = getIndexType(index);
		ObjectTypeTable objectTypeTable = objTableDefs.get(tableName);
		for (String type : objectTypeTable.types) {
			Map<String, OpIndexColumn> indexesByType = indexes.get(type);
			// potentially concurrent modification exception
			indexesByType.remove(index);
		}
		jdbcTemplate.execute("DROP INDEX " + dbschema.generateIndexName(it, tableName, colName));
		CommonPreference<Object> pref = settingsManager.getPreferenceByKey(SettingsManager.DB_SCHEMA_INTERNAL_INDEXES.getId(tableName + "." + colName));
		if(pref != null) {
			settingsManager.removePreferenceInternal(pref);
		}
	}

	private void addIndexCol(OpIndexColumn indexColumn) {
		if (!indexes.containsKey(indexColumn.getOpType())) {
			indexes.put(indexColumn.getOpType(), new TreeMap<String, OpIndexColumn>());
		}
		indexes.get(indexColumn.getOpType()).put(indexColumn.getIndexId(), indexColumn);
	}


	

	public boolean setSetting(JdbcTemplate jdbcTemplate, String key, String v) {
		return dbschema.setSetting(jdbcTemplate, key, v);
	}
	
	public int removeSetting(JdbcTemplate jdbcTemplate, String key) {
		return dbschema.removeSetting(jdbcTemplate, key);
	}

	public String getSetting(JdbcTemplate jdbcTemplate, String key) {
		return dbschema.getSetting(jdbcTemplate, key);
	}
	
	public void alterTableNewColumn(JdbcTemplate jdbcTemplate, ColumnDef col) {
		dbschema.alterTableNewColumn(jdbcTemplate, col);
	}
	
	public Map<String, String> getSettings(JdbcTemplate jdbcTemplate) {
		Map<String, String> r = new TreeMap<>();
		try {
			jdbcTemplate.query("select key, value from " + SETTINGS_TABLE, new ResultSetExtractor<Void>() {
				@Override
				public Void extractData(ResultSet rs) throws SQLException, DataAccessException {
					while(rs.next()) {
						r.put(rs.getString(1), rs.getString(2));
					}
					return null;
				}
			});
		} catch (DataAccessException e) {
		}
		return r;
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

	public void insertObjIntoTableBatch(List<Object[]> args, String table, JdbcTemplate jdbcTemplate, Collection<OpIndexColumn> indexes) {
		StringBuilder extraColumnNames = new StringBuilder();
		for(OpIndexColumn index : indexes) {
			extraColumnNames.append(index.getColumnDef().getColName()).append(",");
		}
		jdbcTemplate.batchUpdate("INSERT INTO " + table
				+ "(type,ophash,superblock,sblockid,sorder,content,"
				+ extraColumnNames.toString()
				+ generatePKString(table, "p%1$d", ",") + ") "
				+ " values(?,?,?,?,?,?," + repeatString("?,", "", indexes.size()) + generatePKString(table, "?", ",") + ")", args);
	}

	public void insertObjIntoHistoryTableBatch(List<Object[]> args, String table, JdbcTemplate jdbcTemplate) {
		jdbcTemplate.batchUpdate("INSERT INTO " + table + "(blockhash, ophash, type, time, obj, status," +
				generatePKString(table, "usr_%1$d, login_%1$d", ",", HISTORY_USERS_SIZE) + "," +
				generatePKString(table, "p%1$d", ",", MAX_KEY_SIZE) + ") VALUES ("+
				generatePKString(table, "?", ",", HISTORY_USERS_SIZE * 2 + MAX_KEY_SIZE + 6 ) + ")", args);
	}

	


}
