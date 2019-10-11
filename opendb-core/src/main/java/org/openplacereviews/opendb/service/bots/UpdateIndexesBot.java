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
import org.openplacereviews.opendb.util.JsonFormatter;
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
	private JsonFormatter formatter;

	@Autowired
	private BlocksManager blocksManager;

	public UpdateIndexesBot(OpObject botObject) {
		super(botObject, true);
	}

	@Override
	public UpdateIndexesBot call() throws Exception {
		addNewBotStat();
		try {
			info("Start Indexes updating...");
			Map<String, Map<String, Object>> userIndexMap = generateMapFromList(settingsManager.getPreferencesByPrefix(SettingsManager.DB_INDEX_STATE));
			Map<String, Map<String, Object>> currentIndexMap = generateMapFromList(settingsManager.getPreferencesByPrefix(SettingsManager.DB_SCHEMA_INDEXES));

			for (String dbIndex : userIndexMap.keySet()) {
				if (currentIndexMap.containsKey(dbIndex)) {
					String indexName = (String) userIndexMap.get(dbIndex).get(INDEX_NAME);
					info("Start checking index: " + indexName + " on changes ...");
					validatePreferencesOnChanges(userIndexMap.get(dbIndex), currentIndexMap.get(dbIndex));
					info("Checking index: " + indexName + " on changes was finished");
				} else {
					String indexName = (String) userIndexMap.get(dbIndex).get(INDEX_NAME);
					String tableName = (String) userIndexMap.get(dbIndex).get(INDEX_TABLENAME);
					String objType = dbSchemaManager.getTypeByTable(tableName);
					TreeMap<String, Map<String, OpIndexColumn>> indexes = dbSchemaManager.getIndexes();

					info("Found index for removing: '" + indexName + "' for table: " + tableName + " ...");
					dbSchemaManager.removeIndex(jdbcTemplate, generateIndexName(userIndexMap.get(dbIndex)));
					Map<String, OpIndexColumn> tableIndexes = indexes.get(objType);
					List<String> indexesForRemoving = new ArrayList<>();
					for (Map.Entry<String, OpIndexColumn> entry : tableIndexes.entrySet()) {
						if (entry.getValue().getIndexId().equals(indexName)) {
							indexesForRemoving.add(entry.getKey());
						}
					}
					for (String key : indexesForRemoving) {
						tableIndexes.remove(key);
					}
					info("Index: " + indexName + " was removed");
				}
			}

			for (String currentIndex : currentIndexMap.keySet()) {
				if (!userIndexMap.containsKey(currentIndex)) {
					String tableName = (String) currentIndexMap.get(currentIndex).get(INDEX_TABLENAME);
					String colName = (String) currentIndexMap.get(currentIndex).get(INDEX_NAME);
					String index = (String) currentIndexMap.get(currentIndex).get(INDEX_INDEX_TYPE);
					String sqlType = (String) currentIndexMap.get(currentIndex).get(INDEX_SQL_TYPE);

					info("Start creating new index: '" + index + "' for column: " + colName + " ...");
					dbSchemaManager.generateIndexColumn(currentIndexMap.get(currentIndex));
					ColumnDef.IndexType di = dbSchemaManager.getIndexType(index);

					ColumnDef columnDef = new ColumnDef(tableName, colName, null, di);
					createColumnForIndex(tableName, colName, sqlType);
					jdbcTemplate.execute(dbSchemaManager.generateIndexQuery(columnDef));
					info("Index: '" + index + "' for table: " + tableName + " was added");
					info(" Start data migration for new index ...");
					blocksManager.lockBlockchain("Locked for DB Index Updating");

					String objType = dbSchemaManager.getTypeByTable(tableName);
					Collection<OpIndexColumn> indexes = dbSchemaManager.getIndicesForType(objType);
					List<OpIndexColumn> dbIndexesUpdate = new ArrayList<OpIndexColumn>();
					for (OpIndexColumn opIndexColumn : indexes) {
						if (opIndexColumn.getIndexId().equals(colName)) {
							dbIndexesUpdate.add(opIndexColumn);
						}
					}

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

					blocksManager.unlockBlockchain();
					info(" Data migration for new index was finished");
				}
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

	private void insertIndexesIntoTable(List<Object[]> args, String table, JdbcTemplate jdbcTemplate, Collection<OpIndexColumn> indexes) {
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

	private void validatePreferencesOnChanges(Map<String, Object> dbIndex, Map<String, Object> currentIndex) throws Exception {
		Integer dbIndexCacheDbMax = ((Number) dbIndex.get(INDEX_CACHE_DB_MAX)).intValue();
		int currIndexCacheDbMax = ((Number) currentIndex.get(INDEX_CACHE_DB_MAX)).intValue();
		Integer dbIndexCacheRuntimeMax = ((Number) dbIndex.get(INDEX_CACHE_RUNTIME_MAX)).intValue();
		int currIndexCacheRuntimeMax = ((Number) currentIndex.get(INDEX_CACHE_RUNTIME_MAX)).intValue();
		List<String> dbIndexField = (List<String>) dbIndex.get(INDEX_FIELD);
		List<String> currIndexField = (List<String>) currentIndex.get(INDEX_FIELD);

		if (!dbIndexCacheDbMax.equals(currIndexCacheDbMax) || !dbIndexCacheRuntimeMax.equals(currIndexCacheRuntimeMax) ||
				!dbIndexField.equals(currIndexField)) {
			String tableName = (String) dbIndex.get(INDEX_TABLENAME);
			String indexName = (String) dbIndex.get(INDEX_NAME);
			info("Updating index: " + indexName + " for table: " + tableName + " ...");

			TreeMap<String, Map<String, OpIndexColumn>> indexes = dbSchemaManager.getIndexes();
			Map<String, OpIndexColumn> tableIndexes = indexes.get(dbSchemaManager.getTypeByTable(tableName));
			for (Map.Entry<String, OpIndexColumn> opIndexColumn : tableIndexes.entrySet()) {
				OpIndexColumn indexColumn = opIndexColumn.getValue();
				if (indexColumn.getIndexId().equals(indexName)) {
					indexColumn.setCacheDBBlocks(currIndexCacheDbMax);
					indexColumn.setCacheRuntimeBlocks(currIndexCacheRuntimeMax);
					indexColumn.setFieldsExpression(currIndexField);
				}
			}
			info("Updating index: " + indexName + " for table: " + tableName + " was finished");

		}

		String dbIndexId = (String) dbIndex.get(INDEX_INDEX_TYPE);
		String currIndexId = (String) currentIndex.get(INDEX_INDEX_TYPE);
		String dbIndexName = (String) dbIndex.get(INDEX_NAME);
		String currIndexName = (String) currentIndex.get(INDEX_NAME);
		String dbIndexSqlmapping = (String) dbIndex.get("sqlmapping");
		String currIndexSqlmapping = (String) currentIndex.get("sqlmapping");
		String dbIndexSqltype = (String) dbIndex.get(INDEX_SQL_TYPE);
		String currIndexSqltype = (String) currentIndex.get(INDEX_SQL_TYPE);
		if (!dbIndexId.equals(currIndexId) || !dbIndexName.equals(currIndexName) || !dbIndexSqlmapping.equals(currIndexSqlmapping) ||
				!dbIndexSqltype.equals(currIndexSqltype)) {
			throw new IllegalArgumentException("Cannot to change fields: index, name, sqlmapping, sqltype!!! Please, remove index and create new!!");
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

	private Map<String, Map<String, Object>> generateMapFromList(List<SettingsManager.CommonPreference<Map<String, Object>>> list) {
		Map<String, Map<String, Object>> map = new HashMap<>();
		for (SettingsManager.CommonPreference<Map<String, Object>> object : list) {
			map.put(settingsManager.DB_SCHEMA_INDEXES.getId(object.getValue()), object.getValue());
		}

		return map;
	}

	private void createColumnForIndex(String tableName, String colName, String type) {
		jdbcTemplate.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + colName + " " + type + ";");
	}

	private String generateIndexName(Map<String, Object> indexInfo) {
		String indexType = (String) indexInfo.get(INDEX_INDEX_TYPE);
		String tableName = (String) indexInfo.get(INDEX_TABLENAME);
		String colName = (String) indexInfo.get(INDEX_NAME);
		switch (indexType) {
			case "true": {
				return String.format("%s_%s_ind", tableName, colName);
			}
			case "GIN": {
				return String.format("%s_%s_gin_ind", tableName, colName);
			}
			case "GIST": {
				return String.format("%s_%s_gist_ind", tableName, colName);
			}
			default: {
				return "";
			}
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
