package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;

import static org.openplacereviews.opendb.service.BlocksManager.BLOCKCHAIN_SETTINGS;

@Service
public class SettingsManager {

	
	public static final String OBJTABLE_PROPERTY_NAME = "opendb.db-schema.objtables";
	public static final String OBJTABLE_TYPES = "types";
	public static final String OBJTABLE_KEYSIZE = "keysize";
	public static final String OBJTABLE_TABLENAME = "tablename";
	
	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private JsonFormatter jsonFormatter;

	private Map<String, CommonPreference<?>> preferences = new LinkedHashMap<>();
	
	private boolean dbValueLoaded;
	
	public void initPreferences() {
		// TODO get from env variable
		// TODO support OBJTABLE_PROPERTY_NAME list
		dbValueLoaded = true;
		Map<String, String> ms = dbSchemaManager.getSettings(jdbcTemplate);
		for(String k : ms) {
			
		}
		if (settings != null) {
			for(Map.Entry<String, Map<?, ?>> opendbPreferenceEntry : jsonFormatter.fromJsonToOpendbPreferenceMap(settings).entrySet()) {
				CommonPreference<?> preference = preferences.get(opendbPreferenceEntry.getKey());
				if (preference != null) {
					Object value = opendbPreferenceEntry.getValue().get("value");
					value = generateObjectByType(value, (String) opendbPreferenceEntry.getValue().get("type"));
					boolean updated = updatePreference(opendbPreferenceEntry.getKey(), value);
					if (!updated) {
						throw new IllegalArgumentException("Settings was not loaded");
					}
				}
			}
		}
	}

	public boolean savePreferences(String s, Object o) {
		Map<String, CommonPreference<?>> mapForStoring = new HashMap<>();
		for (Map.Entry<String, CommonPreference<?>> entry : preferences.entrySet()) {
			if (entry.getValue().isCanEdit() || entry.getValue() == OPENDB_BLOCKCHAIN_STATUS) {
				mapForStoring.put(entry.getKey(), entry.getValue());
			}
		}
		return dbSchemaManager.setSetting(jdbcTemplate, PREFERENCES_NAME, jsonFormatter.fullObjectToJson(mapForStoring));
	}

	public Collection<CommonPreference<?>> getPreferences() {
		return preferences.values();
	}
	
	
	@SuppressWarnings("unchecked")
	public <T> List<CommonPreference<T>> getPreferencesByPrefix(String keyPrefix) {
		List<CommonPreference<T>> opendbPreferences = new ArrayList<>();
		if(!keyPrefix.endsWith(".")) {
			keyPrefix = keyPrefix + ".";
		}
		for (String key : preferences.keySet()) {
			if (key.startsWith(keyPrefix)) {
				opendbPreferences.add((CommonPreference<T>) preferences.get(key));
			}
		}

		return opendbPreferences;
	}

	@SuppressWarnings("unchecked")
	public <T> CommonPreference<T> getPreferenceByKey(String keyPreference) {
		return (CommonPreference<T>) preferences.get(keyPreference);
	}

	/////////////// PREFERENCES classes ////////////////
	public class CommonPreference<T> {
		private final String id;
		protected T value;
		private String description;
		private boolean restartIsNeeded;
		private boolean canEdit;
		protected Class<T> clz;

		public CommonPreference(Class<T> cl, String id, T value, String description) {
			this.id = id;
			this.clz = cl;
			this.value = value;
			this.description = description;
		}
		
		public CommonPreference<T> editable() {
			this.canEdit = true;
			return this;
		}
		
		public CommonPreference<T> restartNeeded() {
			this.restartIsNeeded = true;
			return this;
		}

		public String getDescription() {
			return description;
		}

		public boolean isCanEdit() {
			return canEdit;
		}

		public boolean restartIsNeeded() {
			return restartIsNeeded;
		}

		protected T getValue() {
			return value;
		}

		public String getId() {
			return id;
		}
		
		public boolean setString(String o) {
			if(clz == Integer.class) {
				return set(clz.cast(Integer.parseInt(o)));
			} else if(clz == Long.class) {
				return set(clz.cast(Long.parseLong(o)));
			} else if(clz == Boolean.class) {
				return set(clz.cast(Boolean.parseBoolean(o)));
			} else if(clz == Double.class) {
				return set(clz.cast(Double.parseDouble(o)));
			} else if(clz == String.class) {
				return set(clz.cast(o));
			} else {
				throw new UnsupportedOperationException("Conversion is unsupported for class: " + clz);
			}
		}
		
		protected Object convertToDBValue(T obj) {
			return obj.toString();
		}

		protected boolean set(T obj) {
			if (value.getClass() == obj.getClass()) {
				savePreferences(id, convertToDBValue(obj));
				value = obj;
				return true;
			}
			return false;
		}

		

		public T get() {
			return value;
		}

	}

	public class MapStringObjectPreference extends CommonPreference<Map<String, Object>> {

		@SuppressWarnings("unchecked")
		private MapStringObjectPreference(String id, Map<String, Object> defaultValue, String description) {
			super((Class<Map<String, Object>>) defaultValue.getClass(), id, defaultValue, description);
		}
		
		@Override
		public boolean setString(String o) {
			return set(jsonFormatter.fromJsonToTreeMap(o));
		}
		
		@Override
		protected Object convertToDBValue(Map<String, Object> obj) {
			return jsonFormatter.fullObjectToJson(obj);
		}
	}
	
	private <T> CommonPreference<T> regPreference(CommonPreference<T> p) {
		if(dbValueLoaded) {
			throw new IllegalStateException("Preferences could be registered only at startup");
		}
		if(preferences.containsKey(p.getId()) ) {
			throw new IllegalStateException("Duplicate preference was registered");
		}
		return p;
	}

	public CommonPreference<Boolean> registerBooleanPreference(String id, boolean defValue, String description) {
		return regPreference(new CommonPreference<>(Boolean.class, id, defValue, description));
	}

	public CommonPreference<String> registerStringPreference(String id, String defValue, String description) {
		return regPreference(new CommonPreference<>(String.class, id, defValue, description));
	}

	public CommonPreference<Integer> registerIntPreference(String id, int defValue, String description) {
		return regPreference(new CommonPreference<>(Integer.class, id, defValue, description));
	}

	public CommonPreference<Long> registerLongPreference(String id, long defValue, String description) {
		return regPreference(new CommonPreference<>(Long.class, id, defValue, description));
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Double> registerDoublePreference(String id, double defValue, String description) {
		return regPreference(new CommonPreference<>(Double.class, id, defValue, description));
	}

	
	public CommonPreference<Map<String, Object>> registerTableMapping(String tableName,
			int keysize, String... types) {
		Map<String, Object> mp = new TreeMap<>();
		mp.put(OBJTABLE_TYPES, Arrays.asList(types));
		mp.put(OBJTABLE_KEYSIZE, keysize);
		mp.put(OBJTABLE_TABLENAME, tableName);
		String id =  OBJTABLE_PROPERTY_NAME + "."+tableName;
		String description = "DB table to store " + Arrays.asList(types) + " objects";
		return registerMapPreference(id, mp, description).restartNeeded();
	}
	
	@SuppressWarnings("unchecked")
	public CommonPreference<Map<String, Object>> registerMapPreference(String id, Map<String, Object> defValue, String description) {
		if (preferences.containsKey(id)) {
			return (CommonPreference<Map<String, Object>>) preferences.get(id);
		}
		MapStringObjectPreference p = new MapStringObjectPreference(id, defValue, description);
		preferences.put(id, p);
		return p;
	}

	// REPLICA
	public final CommonPreference<Integer> OPENDB_REPLICATE_INTERVAL = registerIntPreference("opendb.replicate.interval", 15, "Time interval to replicate blocks").editable();
	public final CommonPreference<String> OPENDB_REPLICATE_URL = registerStringPreference("opendb.replicate.url", "https://dev.openplacereviews.org/api/", "Main source to replicate blocks").editable();

	// BLOCK AND HISTORY
	public final CommonPreference<Boolean> OPENDB_STORE_HISTORY = registerBooleanPreference("opendb.db.store-history", true, "Store history of operations").editable().restartNeeded();
	public final CommonPreference<Double> OPENDB_COMPACT_COEFICIENT = registerDoublePreference("opendb.db.compactCoefficient", 1.0,  "Compact coefficient for compacting blockchain").editable();
	public final CommonPreference<Integer> OPENDB_SUPERBLOCK_SIZE = registerIntPreference("opendb.db.dbSuperblockSize", 32,  "The amount of blocks to create superblock in a database").editable();

	// LOCAL STORAGE
	public final CommonPreference<String> OPENDB_STORAGE_LOCAL_STORAGE_PATH = registerStringPreference("opendb.storage.local-storage", "", "Path for storing resource files").restartNeeded().editable();
	public final CommonPreference<Integer> OPENDB_STORAGE_TIME_TO_STORE_UNUSED_RESOURCE_SEC = registerIntPreference("opendb.storage.timeToStoreUnusedSec", 86400, "Maximum time (seconds) to storing unused ipfs resources").editable();

	// IPFS SETTINGS
	public final CommonPreference<String> OPENDB_STORAGE_IPFS_NODE_HOST = registerStringPreference("opendb.storage.ipfs.node.host", "", "Hostname to ipfs node").editable();
	public final CommonPreference<Integer> OPENDB_STORAGE_IPFS_NODE_PORT = registerIntPreference("opendb.storage.ipfs.node.port", 5001, "Hostname to ipfs port").editable();
	public final CommonPreference<Integer> OPENDB_STORAGE_IPFS_NODE_READ_TIMEOUT_MS = registerIntPreference("opendb.storage.ipfs.node.readTimeoutMs", 10000, "IPFS: timeout to read from onde").editable();

	// FILE BACKUP
	public final CommonPreference<String> OPENDB_FILE_BACKUP_DIRECTORY = registerStringPreference("opendb.files-backup.directory", "blocks", "Path for storing block backup").restartNeeded().editable();

	// SCHEDULED SERVICE SETTINGS
	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MIS_SECONDS_INTERVAL = registerIntPreference("opendb.block-create.minSecondsInterval", 120, "Min interval between creating blocks").editable();
//	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MIN_QUEUE_SIZE = new IntPreference("opendb.block-create.minQueueSize", 10, false, "", true);
	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MAX_SECONDS_INTERVAL = registerIntPreference("opendb.block-create.maxSecondsInterval", 3600, "Max interval between creating blocks").editable();
	public final CommonPreference<Double> OPENDB_BLOCK_CREATE_MIN_CAPACITY = registerDoublePreference("opendb.block-create.minCapacity", 0.7, "Min capacity for blocks").editable();

	// BOTS
	public final CommonPreference<Integer> OPENDB_BOTS_MIN_INTERVAL = registerIntPreference("opendb.bots.minInterval", 1800, "Min interval to start bots").editable();

	// OBJTABLES SETTINGS
	
	public final CommonPreference<Map<String, Object>> OBJTABLES_LOGINS = registerTableMapping("obj_logins", 2, "sys.login, sys.signup");
	public final CommonPreference<Map<String, Object>> OBJTABLES_GRANTS = registerTableMapping("obj_grants", 2, "sys.grant");
	public final CommonPreference<Map<String, Object>> OBJTABLES_SYSTEM = registerTableMapping("obj_system", 1, "sys.validate", "sys.operation", "sys.role");

	// BLOCKCHAIN STATUS
	public final CommonPreference<String> OPENDB_BLOCKCHAIN_STATUS = registerStringPreference(BLOCKCHAIN_SETTINGS, "none", "Blockchain status (none, createblocks, replicate)");

}
