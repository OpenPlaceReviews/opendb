package org.openplacereviews.opendb.service.bots;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.opendb.service.SettingsManager;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.service.SettingsManager.PreferenceFamily;
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
	

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private SettingsManager settingsManager;

	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Autowired
	private BlocksManager blocksManager;

	public UpdateIndexesBot(String id) {
		super(id);
	}

	@Override
	public UpdateIndexesBot call() throws Exception {
		addNewBotStat();
		try {
			info("Start updating indexes...");
			Map<String, CommonPreference<Map<String, Object>>> expectedIndexState = getState(SettingsManager.DB_SCHEMA_INDEXES);
			Map<String, CommonPreference<Map<String, Object>>> actualIndexState = getState(SettingsManager.DB_SCHEMA_INTERNAL_INDEXES);

			for (String indexId : actualIndexState.keySet()) {
				Map<String, Object> currentIndex = actualIndexState.get(indexId).get();
				CommonPreference<Map<String, Object>> userInputPref = expectedIndexState.remove(indexId);
				String indexName = (String) currentIndex.get(INDEX_NAME);
				if (userInputPref != null) {
					Map<String, Object> userInput = userInputPref.get();
					info("Start checking index: " + indexName + " on changes ...");
					if(validatePreferencesOnChanges(currentIndex, userInput)) {
						actualIndexState.get(indexId).set(userInput);
						info("Saving index state: " + indexName );
					}
					info("Checking index: " + indexName + " on changes was finished");
				} else {
					dbSchemaManager.removeIndex(jdbcTemplate, currentIndex);
					info("Index: " + indexName + " was removed");
				}
			}

			for (String indexId : expectedIndexState.keySet()) {
				Map<String, Object> expectedIndex = expectedIndexState.remove(indexId).get();
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
					if(map != null && map.get(colName) != null) {
						reindexOpColumn(tableName, objType, map.get(colName));
					}
				}
				blocksManager.unlockBlockchain();
				CommonPreference<Map<String, Object>> pn = settingsManager.registerMapPreferenceForFamily(SettingsManager.DB_SCHEMA_INTERNAL_INDEXES, expectedIndex);
				pn.set(expectedIndex);
				info("Data migration for new index was finished");
			}
			setSuccessState();
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


	private Map<String, CommonPreference<Map<String, Object>>> getState(PreferenceFamily dbSchemaIndexes) {
		Map<String, CommonPreference<Map<String, Object>>> mp = new TreeMap<>();
		List<CommonPreference<Map<String, Object>>> l = settingsManager.getPreferencesByPrefix(dbSchemaIndexes);
		for(CommonPreference<Map<String, Object>> p : l) {
			mp.put((String)p.get().get(INDEX_NAME), p);
		}
		return mp;
	}

	private void reindexOpColumn(String tableName, String objType, OpIndexColumn ind) {
		String sql = "UPDATE " + tableName + " SET " + ind.getColumnDef().getColName() + " = ? WHERE "
				+ dbSchemaManager.generatePKString(tableName, "p%1$d = ?", " AND ") + " AND ophash = ?";
		
		OpBlockChain opBlockChain = blocksManager.getBlockchain();
		while (opBlockChain.getParent() != null) {
			Stream<Map.Entry<CompoundKey, OpObject>> objects = opBlockChain.getRawSuperblockObjects(objType);
			List<Object[]> args = prepareInsertIndexObjBatch(objects, objType, ind);
			totalCnt += args.size();
			for (Object[] arg : args) {
				progress++;
				jdbcTemplate.update(sql, arg);
				if (progress % 5000 == 0) {
					info(String.format("Progress of 'update-indexes' %d / %d", progress, totalCnt));
				}
			}

			opBlockChain = opBlockChain.getParent();
		}
	}
	
	private List<Object[]> prepareInsertIndexObjBatch(Stream<Map.Entry<CompoundKey, OpObject>> objects, String type, OpIndexColumn index) {
		List<Object[]> updateList = new ArrayList<>();
		String table = dbSchemaManager.getTableByType(type);
		int ksize = dbSchemaManager.getKeySizeByTable(table);
		Iterator<Map.Entry<CompoundKey, OpObject>> it = objects.iterator();
		Connection conn = null;
		try {
			conn = jdbcTemplate.getDataSource().getConnection();
			while (it.hasNext()) {
				Map.Entry<CompoundKey, OpObject> e = it.next();
				CompoundKey pkey = e.getKey();
				OpObject obj = e.getValue();
				Object[] args = new Object[1 + ksize + 1];
				if (obj != null && !obj.isDeleted()) {
					args[0] = index.evalDBValue(obj, conn);
				}
				pkey.toArray(args, 1);
				args[ksize + 1] = SecUtils.getHashBytes(obj.getParentHash());
				updateList.add(args);
				
			}
		} catch (SQLException e) {
			throw new IllegalArgumentException();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					throw new IllegalArgumentException(e);
				}
			}
		}

		return updateList;
	}

	
	private boolean compare(Map<String, Object> dbIndex, Map<String, Object> currentIndex, String field) {
		Object o1 = dbIndex.get(field);
		Object o2 = currentIndex.get(field);
		return OUtils.equalsStringValue(o1, o2);
	}

	private boolean validatePreferencesOnChanges(Map<String, Object> actual, Map<String, Object> expected)
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
			return true;
		}
		if (!compare(actual, expected, INDEX_INDEX_TYPE) || !compare(actual, expected, INDEX_NAME)
				|| !compare(actual, expected, INDEX_FIELD) || !compare(actual, expected, "sqlmapping")
				|| !compare(actual, expected, INDEX_SQL_TYPE)) {
			throw new IllegalArgumentException(
					"Cannot to change fields: index, name, sqlmapping, sqltype!!! Please, remove index and create new!!");
		}
		return false;

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
