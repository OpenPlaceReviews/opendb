package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;


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
		// load extra properties from DB
		Map<String, String> dbPrefs = dbSchemaManager.getSettings(jdbcTemplate);
		for (String k : dbPrefs.keySet()) {
			if (k.startsWith(OBJTABLE_PROPERTY_NAME + ".") && !preferences.containsKey(k)) {
				TreeMap<String, Object> mp = jsonFormatter.fromJsonToTreeMap(dbPrefs.get(k));
				String d = "DB table to store " + mp.get(OBJTABLE_TYPES) + " objects";
				registerMapPreference(k, mp, d).restartNeeded();
			}
		}
		dbValueLoaded = true;
		for(String k : preferences.keySet()) {
			CommonPreference<?> c = preferences.get(k);
			String envVar = System.getenv(k);
			try {
				if(envVar != null) {
					c.setString(envVar, false);
					c.setSource(CommonPreferenceSource.ENV);
				} else if(dbPrefs.containsKey(k)) {
					c.setString(dbPrefs.get(k), false);
				}
			} catch (Exception e) {
				throw new IllegalStateException(
						String.format("Error setting setting '%s'", k) , e);
			}
			
		}
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
	
	public enum CommonPreferenceSource {
		DEFAULT,
		DB,
		ENV
	}

	/////////////// PREFERENCES classes ////////////////
	public class CommonPreference<T> {
		private final String id;
		protected T value;
		private String description;
		private boolean restartIsNeeded;
		private boolean canEdit;
		protected Class<T> clz;
		protected CommonPreferenceSource source = CommonPreferenceSource.DEFAULT;

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
		
		public void setSource(CommonPreferenceSource source) {
			this.source = source;
		}
		
		public CommonPreferenceSource getSource() {
			return source;
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
			return setString(o, true);
		}
		
		public boolean setString(String o, boolean saveDb) {
			if(clz == Integer.class) {
				return set(clz.cast(Integer.parseInt(o)), saveDb);
			} else if(clz == Long.class) {
				return set(clz.cast(Long.parseLong(o)), saveDb);
			} else if(clz == Boolean.class) {
				return set(clz.cast(Boolean.parseBoolean(o)), saveDb);
			} else if(clz == Double.class) {
				return set(clz.cast(Double.parseDouble(o)), saveDb);
			} else if(clz == String.class) {
				return set(clz.cast(o), saveDb);
			} else {
				throw new UnsupportedOperationException("Conversion is unsupported for class: " + clz);
			}
		}
		
		protected String convertToDBValue(T obj) {
			return obj.toString();
		}

		protected boolean set(T obj, boolean saveDB) {
			if (value.getClass() == obj.getClass()) {
				if(saveDB) {
					dbSchemaManager.setSetting(jdbcTemplate, id, convertToDBValue(obj));
				}
				value = obj;
				return true;
			}
			return false;
		}

		

		public T get() {
			return value;
		}

	}

	public class EnumPreference<T extends Enum<T>> extends CommonPreference<T> {

		public EnumPreference(Class<T> cl, String id, T value, String description) {
			super(cl, id, value, description);
		}
		
		@Override
		public boolean setString(String o, boolean saveDB) {
			return set(Enum.valueOf(clz, o), saveDB);
		}
		
	}
	
	public class MapStringObjectPreference extends CommonPreference<Map<String, Object>> {

		@SuppressWarnings("unchecked")
		private MapStringObjectPreference(String id, Map<String, Object> defaultValue, String description) {
			super((Class<Map<String, Object>>) defaultValue.getClass(), id, defaultValue, description);
		}
		
		@Override
		public boolean setString(String o, boolean saveDB) {
			return set(jsonFormatter.fromJsonToTreeMap(o), saveDB);
		}
		
		@Override
		protected String convertToDBValue(Map<String, Object> obj) {
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
	
	public <T extends Enum<T>> CommonPreference<T> registerEnumPreference(Class<T> cl, String id, T value, String description) {
		EnumPreference<T> et = new EnumPreference<T>(cl, id, value, description);
		return regPreference(et);
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
	public enum BlockSource {
		NONE,
		REPLICATION,
		CREATE
	}
	public final CommonPreference<BlockSource> OPENDB_BLOCKCHAIN_STATUS = registerEnumPreference(BlockSource.class, "opendb.blockchain-status", BlockSource.NONE, "Block source (none, replicate, create)");

}
