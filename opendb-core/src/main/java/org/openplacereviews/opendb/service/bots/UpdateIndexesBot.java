package org.openplacereviews.opendb.service.bots;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.ColumnDef;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.openplacereviews.opendb.service.SettingsManager;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static org.openplacereviews.opendb.service.SettingsManager.*;

public class UpdateIndexesBot extends GenericMultiThreadBot<UpdateIndexesBot> {

	private int totalCnt = 1;
	private int progress = 0;
	
	public static final String INDEX_CURRENT_STATE = "opendb.current-index.state";

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private SettingsManager settingsManager;

	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private BlocksManager blocksManager;

	public UpdateIndexesBot(String id) {
		super(id);
	}

	@SuppressWarnings("unchecked")
	@Override
	public UpdateIndexesBot call() throws Exception {
		addNewBotStat();
		try {
			info("Start Indexes updating...");
			Map<String, Map<String, Object>> expectedIndexState = new LinkedHashMap<String, Map<String,Object>>();
			Map<String, Map<String, Object>> actualIndexState = new LinkedHashMap<String, Map<String,Object>>();
			List<CommonPreference<Map<String, Object>>> userIndexes = settingsManager.getPreferencesByPrefix(SettingsManager.DB_SCHEMA_INDEXES);
			for(CommonPreference<Map<String, Object>> p : userIndexes) {
				expectedIndexState.put(p.getId(), p.get());
			}
			String s = dbSchemaManager.getSetting(jdbcTemplate, INDEX_CURRENT_STATE);
			if(s == null ) {
				TreeMap<String, Object> mp = formatter.fromJsonToTreeMap(s);
				for(String k : mp.keySet()) {
					actualIndexState.put(k, (Map<String, Object>) mp.get(k));
				}
			}

			for (String indexId : actualIndexState.keySet()) {
				Map<String, Object> currentIndex = actualIndexState.get(indexId);
				Map<String, Object> userInput = expectedIndexState.remove(indexId);
				String indexName = (String) currentIndex.get(INDEX_NAME);
				String tableName = (String) currentIndex.get(INDEX_TABLENAME);
				if (userInput != null) {
					info("Start checking index: " + indexName + " on changes ...");
					validatePreferencesOnChanges(currentIndex, userInput);
					info("Checking index: " + indexName + " on changes was finished");
				} else {
					dbSchemaManager.removeIndex(jdbcTemplate, currentIndex);
					info("Index: " + indexName + " was removed");
				}
			}

			for (String indexId : expectedIndexState.keySet()) {
				Map<String, Object> expectedIndex = expectedIndexState.remove(indexId);
				String colName = (String) expectedIndex.get(INDEX_NAME);
				String tableName = (String) expectedIndex.get(INDEX_TABLENAME);
				
				info(String.format("Start creating new index: '%s' for table '%s'...", colName, tableName));
				dbSchemaManager.alterTableNewColumn(jdbcTemplate, dbSchemaManager.generateIndexColumn(expectedIndex));
				info("Index: '" + colName + "' for table: " + tableName + " was added");
				
				info(" Start data migration for new index ...");
				blocksManager.lockBlockchain("Locked for DB Index Updating");
				List<String> objTypes = dbSchemaManager.getTypesByTable(tableName);
				for (String objType : objTypes) {
					Map<String, OpIndexColumn> map = dbSchemaManager.getIndexes().get(objType);
					if(map != null) {
						reindexOpColumn(tableName, objType, map.get(colName));
					}
				}
				
				blocksManager.unlockBlockchain();
				info("Data migration for new index was finished");
			}

			setSuccessState();
			settingsManager.saveCurrentDbIndexes();
			info("Updating Indexes is finished");
		} catch (Exception e) {
			setFailedState();
			info("Index updating has failed: " + e.getMessage(), e);
			throw e;
		} finally {
			super.shutdown();
		}
		return this;
	}

	private void reindexOpColumn(String tableName, String objType, OpIndexColumn indCol) {
		OpBlockChain opBlockChain = blocksManager.getBlockchain();
		while (opBlockChain.getParent() != null) {
			Stream<Map.Entry<CompoundKey, OpObject>> objects = opBlockChain.getRawSuperblockObjects(objType);
			List<Object[]> insertBatch = prepareInsertIndexObjBatch(objects, objType, dbIndexesUpdate);
			if (insertBatch.size() > 0) {
				totalCnt += insertBatch.size();
				insertIndexesIntoTable(insertBatch, tableName, jdbcTemplate, dbIndexesUpdate);
			}

			opBlockChain = opBlockChain.getParent();
		}
	}
	
	private List<Object[]> prepareInsertIndexObjBatch(Stream<Map.Entry<CompoundKey, OpObject>> objects, String type, Collection<OpIndexColumn> indexes) {
		List<Object[]> updateList = new ArrayList<>();
		int ksize = dbSchemaManager.getKeySizeByType(type);
		Iterator<Map.Entry<CompoundKey, OpObject>> it = objects.iterator();
		try {
			Connection conn = jdbcTemplate.getDataSource().getConnection();
			while (it.hasNext()) {
				Map.Entry<CompoundKey, OpObject> e = it.next();
				CompoundKey pkey = e.getKey();
				OpObject obj = e.getValue();

				Object[] args = new Object[indexes.size() + ksize + 1];
				int ind = 0;

				for (OpIndexColumn index : indexes) {
					if (!obj.isDeleted()) {
						args[ind++] = index.evalDBValue(obj, conn);
					} else {
						args[ind++] = null;
					}
				}
				pkey.toArray(args, ind);
				ind += ksize;
				args[ind] = SecUtils.getHashBytes(obj.getParentHash());

				updateList.add(args);
			}
			conn.close();
		} catch (SQLException e) {
			throw new IllegalArgumentException();
		}

		return updateList;
	}

	private void insertIndexesIntoTable(List<Object[]> args, String table, JdbcTemplate jdbcTemplate, OpIndexColumn ind) {
		StringBuilder extraColumnNames = new StringBuilder();
		for (OpIndexColumn index : indexes) {
			if (extraColumnNames.length() == 0) {
				extraColumnNames.append(index.getColumnDef().getColName()).append(" = ?");
			} else {
				extraColumnNames.append(", ").append(index.getColumnDef().getColName()).append(" = ?");
			}
		}
		for (Object[] arg : args) {
			progress++;
			jdbcTemplate.update("UPDATE " + table +
					" SET " + extraColumnNames.toString() + " WHERE " + dbSchemaManager.generatePKString(table, "p%1$d = ?", " AND ") + " AND ophash = ?", arg
			);

			if (progress % 5000 == 0) {
				info(String.format("Progress of 'update-indexes' %d / %d", progress, totalCnt));
			}
		}
	}
	
	private boolean compare(Map<String, Object> dbIndex, Map<String, Object> currentIndex, String field) {
		Object o1 = dbIndex.get(field);
		Object o2 = currentIndex.get(field);
		return OUtils.equalsStringValue(o1, o2);
	}

	private void validatePreferencesOnChanges(Map<String, Object> actual, Map<String, Object> expected)
			throws Exception {
		if (!compare(actual, expected, INDEX_CACHE_DB_MAX) || !compare(actual, expected, INDEX_CACHE_RUNTIME_MAX)) {
			String tableName = (String) actual.get(INDEX_TABLENAME);
			String indexName = (String) actual.get(INDEX_NAME);
			info("Updating cache size index: " + indexName + " for table: " + tableName + " ...");
			List<String> types = dbSchemaManager.getTypesByTable(tableName);
			Map<String, Map<String, OpIndexColumn>> indexesByTypes = dbSchemaManager.getIndexes();
			for (String type : types) {
				Map<String, OpIndexColumn> indexes = indexesByTypes.get(type);
				OpIndexColumn indexColumn = indexes.get(indexName);
				if (indexColumn != null) {
					if (expected.get(INDEX_CACHE_DB_MAX) != null) {
						indexColumn.setCacheDBBlocks(((Number) expected.get(INDEX_CACHE_DB_MAX)).intValue());
					}
					if (expected.get(INDEX_CACHE_RUNTIME_MAX) != null) {
						indexColumn.setCacheRuntimeBlocks(((Number) expected.get(INDEX_CACHE_RUNTIME_MAX)).intValue());
					}
				}
			}
			info("Updating index: " + indexName + " for table: " + tableName + " was finished");
		}
		if (!compare(actual, expected, INDEX_INDEX_TYPE) || !compare(actual, expected, INDEX_NAME)
				|| !compare(actual, expected, INDEX_FIELD) || !compare(actual, expected, "sqlmapping")
				|| !compare(actual, expected, INDEX_SQL_TYPE)) {
			throw new IllegalArgumentException(
					"Cannot to change fields: index, name, sqlmapping, sqltype!!! Please, remove index and create new!!");
		}

	}

	@Override
	public int total() {
		return totalCnt;
	}

	@Override
	public int progress() {
		return progress;
	}

	@Override
	public String getTaskDescription() {
		return "Updating DB indexes";
	}

	@Override
	public String getTaskName() {
		return "update-indexes";
	}

}
