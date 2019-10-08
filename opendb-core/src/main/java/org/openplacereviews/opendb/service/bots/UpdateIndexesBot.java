package org.openplacereviews.opendb.service.bots;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpIndexColumn;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.de.ColumnDef;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.openplacereviews.opendb.service.SettingsManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.INDEXED;

public class UpdateIndexesBot extends GenericMultiThreadBot<UpdateIndexesBot> {

	private static final Log LOGGER = LogFactory.getLog(UpdateIndexesBot.class);

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
		super(botObject);
	}

	@Override
	public String getTaskDescription() {
		return "Updating DB indexes";
	}

	@Override
	public String getTaskName() {
		return "update-indexes";
	}

	@Override
	public UpdateIndexesBot call() throws Exception {
		addNewBotStat();
		try {
			info("Start Indexes updating...");
			List<SettingsManager.CommonPreference<Map<String, Object>>> dbIndexes = formatter.fromJsonToListTreeMap(dbSchemaManager.getSetting(jdbcTemplate, SettingsManager.DB_SCHEMA_INDEXES.prefix));
			List<SettingsManager.CommonPreference<Map<String, Object>>> currentIndexPrefs = settingsManager.getPreferencesByPrefix(SettingsManager.DB_SCHEMA_INDEXES);

			for (SettingsManager.CommonPreference<Map<String, Object>> index : dbIndexes) {
				boolean indexIsExist = false;
				for (SettingsManager.CommonPreference<Map<String, Object>> currentIndex : currentIndexPrefs) {
					if (index.getId().equals(currentIndex.id)) {
						String indexName = (String) currentIndex.value.get("name");
						info("Start checking index: " + indexName + " on changes ...");
						validatePreferencesOnChanges(index.value, currentIndex.value);
						indexIsExist = true;
						info("Checking index: " + indexName + " on changes was finished");
					}
				}
				if (!indexIsExist) {
					String indexName = (String) index.value.get("name");
					String tableName = (String) index.value.get("tablename");
					info("Found index for removing: '" + indexName + "' for table: " + tableName + " ...");
					dbSchemaManager.removeIndex(jdbcTemplate, generateIndexName(index.value));
					TreeMap<String, Map<String, OpIndexColumn>> indexes = dbSchemaManager.getIndexes();
					Map<String, OpIndexColumn> tableIndexes = indexes.get(tableName);
					List<String> indexesForRemoving = new ArrayList<>();
					for (Map.Entry<String, OpIndexColumn> opIndexColumn : tableIndexes.entrySet()) {
						if (opIndexColumn.getValue().getIndexId().equals(indexName)) {
							indexesForRemoving.add(opIndexColumn.getKey());
						}
					}
					for (String key : indexesForRemoving) {
						tableIndexes.remove(key);
					}
					info("Index: " + indexName + " was removed");
				}
			}

			for (SettingsManager.CommonPreference<Map<String, Object>> currentIndex : currentIndexPrefs) {
				boolean indexIsExist = false;
				for (SettingsManager.CommonPreference<Map<String, Object>> dbIndex : dbIndexes) {
					if (dbIndex.getId().equals(currentIndex.id)) {
						indexIsExist = true;
					}
				}
				if (!indexIsExist) {
					String tableName = (String) currentIndex.value.get("tablename");
					String colName = (String) currentIndex.value.get("name");
					String index = (String) currentIndex.value.get("index");
					String type = (String) currentIndex.value.get("sqltype");
					info("Start creating new index: '" + index + "' for column: " + colName + " ...");
					dbSchemaManager.generateIndexColumn(currentIndex.value);
					ColumnDef.IndexType di = null;
					if (index != null) {
						if (index.equalsIgnoreCase("true")) {
							di = INDEXED;
						} else {
							di = ColumnDef.IndexType.valueOf(index);
						}
					}

					ColumnDef columnDef = new ColumnDef(tableName, colName, null, di);
					createColumnForIndex(tableName, colName, type);
					jdbcTemplate.execute(dbSchemaManager.generateIndexQuery(columnDef));
					info("Index: '" + index + "' for table: " + tableName + " was added");
					info(" Start data migration for new index ...");
					blocksManager.lockBlockchain();
					// TODO fill new index field
					blocksManager.unlockBlockchain();
					info(" Data migration for new index was finished");
				}
			}

			setSuccessState();
			dbSchemaManager.setSetting(jdbcTemplate, SettingsManager.DB_SCHEMA_INDEXES.prefix, formatter.fullObjectToJson(currentIndexPrefs));
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

	private void validatePreferencesOnChanges(Map<String, Object> dbIndex, Map<String, Object> currentIndex) {
		Integer dbIndexCacheDbMax = (Integer) dbIndex.get("cache-db-max");
		Integer currIndexCacheDbMax = (Integer) currentIndex.get("cache-db-max");
		Integer dbIndexCacheRuntimeMax = (Integer) dbIndex.get("cache-runtime-max");
		Integer currIndexCacheRuntimeMax = (Integer) currentIndex.get("cache-runtime-max");
		List<String> dbIndexField = (List<String>) dbIndex.get("field");
		List<String> currIndexField = (List<String>) currentIndex.get("field");

		if (!dbIndexCacheDbMax.equals(currIndexCacheDbMax) || !dbIndexCacheRuntimeMax.equals(currIndexCacheRuntimeMax) ||
				!dbIndexField.equals(currIndexField)) {
			String tableName = (String) dbIndex.get("tablename");
			String indexName = (String) dbIndex.get("name");
			info("Updating index: " + indexName + " for table: " + tableName + " ...");

			TreeMap<String, Map<String, OpIndexColumn>> indexes = dbSchemaManager.getIndexes();
			Map<String, OpIndexColumn> tableIndexes = indexes.get(tableName);
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

		String dbIndexId = (String) dbIndex.get("index");
		String currIndexId = (String) dbIndex.get("index");
		String dbIndexName = (String) dbIndex.get("name");
		String currIndexName = (String) dbIndex.get("name");
		String dbIndexSqlmapping = (String) dbIndex.get("sqlmapping");
		String currIndexSqlmapping = (String) dbIndex.get("sqlmapping");
		String dbIndexSqltype = (String) dbIndex.get("sqltype");
		String currIndexSqltype = (String) dbIndex.get("sqltype");
		if (!dbIndexId.equals(currIndexId) || !dbIndexName.equals(currIndexName) || !dbIndexSqlmapping.equals(currIndexSqlmapping) ||
				!dbIndexSqltype.equals(currIndexSqltype)) {
			// TODO throw exception
			error("Cannot to change fields: index, name, sqlmapping, sqltype!!! Please, remove index and create new!!", new Exception());
		}

	}

	private void createColumnForIndex(String tableName, String colName, String type) {
		jdbcTemplate.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + colName + " " + type + ";");
	}

	private String generateIndexName(Map<String, Object> indexInfo) {
		String indexType = (String) indexInfo.get("index");
		String tableName = (String) indexInfo.get("tablename");
		String colName = (String) indexInfo.get("name");
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
}
