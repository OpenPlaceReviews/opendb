package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


@Service
public class SettingsManager {
	
	public static final String OBJTABLE_TYPES = "types";
	public static final String OBJTABLE_KEYSIZE = "keysize";
	public static final String OBJTABLE_TABLENAME = "tablename";
	
	
	public static final String INDEX_TABLENAME = "tablename";
	public static final String INDEX_NAME = "name";
	public static final String INDEX_SQL_TYPE = "sqltype";
	public static final String INDEX_INDEX_TYPE = "index";
	public static final String INDEX_CACHE_RUNTIME_MAX = "cache-runtime-max";
	public static final String INDEX_CACHE_DB_MAX = "cache-db-max";
	public static final String INDEX_FIELD = "field";
	
	public static final String BOT_ID = "bot_id";
	public static final String BOT_INTERVAL_SECONDS = "interval_sec";
	public static final String BOT_ENABLED = "enabled";
	public static final String BOT_LAST_RUN = "last_run";

	public static final String ENDPOINT_ID = "endpoint_id";
	
	public static final String TABLE_ID = "id";
	
	public static final PreferenceFamily USER = new PreferenceFamily(null, "User");
	public static final PreferenceFamily DB_SCHEMA_OBJTABLES = new PreferenceFamily("opendb.db-schema.objtables", "DB Tables").
			setIdProperties(OBJTABLE_TABLENAME).setDescription("DB config to store %s objects", OBJTABLE_TYPES).setRestartNeeded().canAdd().canEdit().canDelete();
	public static final PreferenceFamily DB_SCHEMA_INDEXES = new PreferenceFamily("opendb.db-schema.indexes", "DB Indexes").
			setIdProperties(INDEX_TABLENAME, INDEX_NAME).setDescription("DB config to describe index %s.%s ", INDEX_TABLENAME, INDEX_NAME).canAdd().canEdit().canDelete();
	public static final PreferenceFamily DB_SCHEMA_INTERNAL_INDEXES = new PreferenceFamily("opendb.db-schema.internal-indexes", "Internal DB Indexes").
			setIdProperties(INDEX_TABLENAME, INDEX_NAME).setDescription("DB config to describe actual index state %s.%s ", INDEX_TABLENAME, INDEX_NAME);
	public static final PreferenceFamily OPENDB_BOTS_CONFIG = new PreferenceFamily("opendb.bots", "Bots").
			setIdProperties(BOT_ID).setDescription("Bot %s configuration", BOT_ID).canEdit().canDelete();
	public static final PreferenceFamily OPENDB_ENDPOINTS_CONFIG = new PreferenceFamily("opendb.publicdata", "Data Endpoints").
			setIdProperties(ENDPOINT_ID).setDescription("Data %s configuration", ENDPOINT_ID).canEdit().canAdd().canDelete();
	
	public static final PreferenceFamily[] SETTINGS_FAMILIES = new PreferenceFamily[] {
			USER,
			DB_SCHEMA_OBJTABLES,
			DB_SCHEMA_INDEXES,
			OPENDB_BOTS_CONFIG,
			OPENDB_ENDPOINTS_CONFIG,
			DB_SCHEMA_INTERNAL_INDEXES // could be disabled to be non-visible
	};
	
	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private JsonFormatter jsonFormatter;

	private Map<String, CommonPreference<?>> preferences = new TreeMap<>();
	
	private boolean dbValueLoaded;
	
	public void initPreferences() {
		// load extra properties from DB
		Map<String, String> dbPrefs = dbSchemaManager.getSettings(jdbcTemplate);
		for (PreferenceFamily family : SETTINGS_FAMILIES) {
			if(family.prefix == null) {
				continue;
			}
			for (String k : dbPrefs.keySet()) {
				if (k.startsWith(family.prefix + ".") && !preferences.containsKey(k)) {
					TreeMap<String, Object> mp = jsonFormatter.fromJsonToTreeMap(dbPrefs.get(k));
					registerMapPreferenceForFamily(family, mp).restartNeeded();
				}
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
					c.setSource(CommonPreferenceSource.DB);
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
	
	public <T> List<CommonPreference<T>> getPreferencesByPrefix(PreferenceFamily f) {
		return getPreferencesByPrefix(f.prefix);
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

	public <T> boolean removePreference(CommonPreference<T> preference) {
		if (preference.family.canDelete) {
			return removePreferenceInternal(preference);
		}
		return false;
	}


	public <T> boolean removePreferenceInternal(CommonPreference<T> preference) {
		if (dbSchemaManager.removeSetting(jdbcTemplate, preference.getId()) > 0) {
			preference.family.version.incrementAndGet();
			Map<String, CommonPreference<?>> nprefs = new TreeMap<>(preferences);
			nprefs.remove(preference.getId());
			preferences = nprefs;
			return true;
		}
		return false;
	}

	private PreferenceFamily getPreferenceFamily(String id) {
		for (PreferenceFamily preferenceFamily : SETTINGS_FAMILIES) {
			if (OUtils.equalsStringValue(preferenceFamily.id, id)) {
				return preferenceFamily;
			}
		}
		return null;
	}

	public boolean addNewPreference(String familyId, String value) {
		PreferenceFamily preferenceFamily = getPreferenceFamily(familyId);
		if (preferenceFamily != null && preferenceFamily.canAdd) {
			preferenceFamily.version.incrementAndGet();
			CommonPreference<Map<String, Object>> commonPreference = registerMapPreferenceForFamily(preferenceFamily,
					jsonFormatter.fromJsonToTreeMap(value));
			commonPreference.setSource(CommonPreferenceSource.DB);
			dbSchemaManager.setSetting(jdbcTemplate, commonPreference.getId(),
					jsonFormatter.fullObjectToJson(commonPreference.value));
			return true;
		}

		return false;
	}
	
	public enum CommonPreferenceSource {
		DEFAULT,
		DB,
		ENV
	}

	/////////////// PREFERENCES classes ////////////////
	
	public static class PreferenceFamily {
		public String id;
		public String prefix;
		public String name;
		public String descriptionFormat;
		public String[] descProperties;
		public String[] idProperties;
		public boolean restartNeeded;
		public boolean canEdit;
		public boolean canDelete;
		public boolean canAdd;
		public AtomicInteger version = new AtomicInteger(); 
		
		public PreferenceFamily(String prefix, String name) {
			this.prefix = prefix;
			this.id = prefix == null ? "" : prefix.replace('.', '_');
			this.name = name;
		}
		
		public PreferenceFamily setIdProperties(String... idProperties) {
			this.idProperties = idProperties;
			return this;
		}
		
		public PreferenceFamily setRestartNeeded() {
			this.restartNeeded = true;
			return this;
		}
		
		public PreferenceFamily canEdit() {
			this.canEdit = true;
			return this;
		}

		public PreferenceFamily canDelete() {
			this.canDelete = true;
			return this;
		}
		
		public PreferenceFamily canAdd() {
			this.canAdd = true;
			return this;
		}
		
		public String getId(String idSuffix) {
			return prefix + "." + idSuffix;
		}
		
		public PreferenceFamily setDescription(String format, String... properties) {
			this.descriptionFormat = format;
			this.descProperties = properties;
			return this;
		}
		
		public String getDescription(Map<String, Object> o) {
			if(descProperties == null) {
				throw new UnsupportedOperationException();
			}
			Object[] vls = new Object[descProperties.length];
			for(int i = 0; i < descProperties.length; i++) {
				vls[i] = o.get(descProperties[i]);
			}
			return String.format(descriptionFormat, vls);
		}
		
		public String getId(Map<String, Object> o) {
			if(idProperties == null) {
				throw new UnsupportedOperationException();
			}
			String v = prefix;
			for(int i = 0; i < idProperties.length; i++) {
				v += "." + o.get(idProperties[i]); 
			}
			return v;
		}
	}
	
	public class CommonPreference<T> {
		protected final String id;
		protected final PreferenceFamily family;
		protected final transient Class<T> clz;
		protected final String type;
		protected T value;
		protected String description;
		protected boolean restartIsNeeded;
		protected boolean canEdit;
		protected CommonPreferenceSource source = CommonPreferenceSource.DEFAULT;
		

		public CommonPreference(PreferenceFamily family, Class<T> cl, String id, T value, String description) {
			this.family = family;
			this.id = id;
			this.clz = cl;
			this.type = clz.getSimpleName();
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


		public boolean restartIsNeeded() {
			return restartIsNeeded;
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
		
		public boolean set(T obj) {
			return set(obj, true);
		}
		
		protected String convertToDBValue(T obj) {
			return obj.toString();
		}

		protected boolean set(T obj, boolean saveDB) {
			if (value.getClass() == obj.getClass()) {
				if(saveDB) {
					if(source == CommonPreferenceSource.ENV) {
						throw new IllegalStateException(
								String.format("Can't overwrite property '%s' set by environment variable", id));
					}
					setSource(CommonPreferenceSource.DB);
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

		public EnumPreference(PreferenceFamily family, Class<T> cl, String id, T value, String description) {
			super(family, cl, id, value, description);
		}
		
		@Override
		public boolean setString(String o, boolean saveDB) {
			return set(Enum.valueOf(clz, o), saveDB);
		}
		
	}
	
	public class MapStringObjectPreference extends CommonPreference<Map<String, Object>> {

		@SuppressWarnings("unchecked")
		private MapStringObjectPreference(PreferenceFamily family, String id, Map<String, Object> defaultValue, String description) {
			super(family, (Class<Map<String, Object>>) defaultValue.getClass(), id, defaultValue, description);
		}
		
		public MapStringObjectPreference setValue(String key, Object value, boolean save) {
			this.value.put(key, value);
			family.version.getAndIncrement();
			if(save) {
				set(this.value, true);
			}
			return this;
		}
		
		public boolean getBoolean(String key, boolean def) {
			Object o = this.value.get(key);
			if(o != null && (o instanceof Boolean)) {
				return (boolean) o;
			}
			return def;
		}
		
		public String getStringValue(String key, String def) {
			Object o = this.value.get(key);
			if(o != null) {
				return o.toString();
			}
			return def;
		}
		
		public long getLong(String key, long def) {
			Object o = this.value.get(key);
			if(o != null && (o instanceof Long)) {
				return (long) o;
			}
			return def;
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
		if(dbValueLoaded && p.family.prefix == null) {
			throw new IllegalStateException("Preferences could be registered only at startup");
		}
		if(preferences.containsKey(p.getId()) ) {
			throw new IllegalStateException("Duplicate preference was registered");
		}
		preferences.put(p.getId(), p);
		return p;
	}
	
	public <T extends Enum<T>> CommonPreference<T> registerEnumPreference(PreferenceFamily family, Class<T> cl, String id, T value, String description) {
		EnumPreference<T> et = new EnumPreference<T>(family, cl, id, value, description);
		return regPreference(et);
	}
	
	public <T extends Enum<T>> CommonPreference<T> registerEnumPreference(Class<T> cl, String id, T value, String description) {
		return registerEnumPreference(USER, cl, id, value, description);
	}

	public CommonPreference<Boolean> registerBooleanPreference(PreferenceFamily family, String id, boolean defValue, String description) {
		return regPreference(new CommonPreference<>(family, Boolean.class, id, defValue, description));
	}
	
	public CommonPreference<Boolean> registerBooleanPreference(String id, boolean defValue, String description) {
		return registerBooleanPreference(USER, id, defValue, description);
	}

	public CommonPreference<String> registerStringPreference(PreferenceFamily family, String id, String defValue, String description) {
		return regPreference(new CommonPreference<>(family, String.class, id, defValue, description));
	}
	
	public CommonPreference<String> registerStringPreference(String id, String defValue, String description) {
		return registerStringPreference(USER, id, defValue, description);
	}
	
	
	public CommonPreference<Integer> registerIntPreference(PreferenceFamily family, String id, int defValue, String description) {
		return regPreference(new CommonPreference<>(family, Integer.class, id, defValue, description));
	}
	
	public CommonPreference<Integer> registerIntPreference(String id, int defValue, String description) {
		return registerIntPreference(USER, id, defValue, description);
	}

	public CommonPreference<Long> registerLongPreference(PreferenceFamily family, String id, long defValue, String description) {
		return regPreference(new CommonPreference<>(family, Long.class, id, defValue, description));
	}

	public CommonPreference<Long> registerLongPreference(String id, long defValue, String description) {
		return registerLongPreference(USER, id, defValue, description);
	}
	
	public CommonPreference<Double> registerDoublePreference(PreferenceFamily family, String id, double defValue, String description) {
		return regPreference(new CommonPreference<>(family, Double.class, id, defValue, description));
	}
	
	public CommonPreference<Double> registerDoublePreference(String id, double defValue, String description) {
		return registerDoublePreference(USER, id, defValue, description);
	}

	
	public CommonPreference<Map<String, Object>> registerTableMapping(String tableName,
			int keysize, String... types) {
		Map<String, Object> mp = new TreeMap<>();
		mp.put(OBJTABLE_TYPES, Arrays.asList(types));
		mp.put(OBJTABLE_KEYSIZE, keysize);
		mp.put(OBJTABLE_TABLENAME, tableName);
		return registerMapPreferenceForFamily(DB_SCHEMA_OBJTABLES, mp);
	}
	
	public CommonPreference<Map<String, Object>> registerMapPreferenceForFamily(PreferenceFamily pf, Map<String, Object> o) {
		CommonPreference<Map<String, Object>> cp = new MapStringObjectPreference(pf, pf.getId(o), o, pf.getDescription(o));
		if(pf.restartNeeded) {
			cp = cp.restartNeeded();
		}
		if(pf.canEdit) {
			cp = cp.editable();
		}
		return regPreference(cp);
	}
	
	public CommonPreference<Map<String, Object>> registerMapPreference(String id, Map<String, Object> defValue, String description) {
		MapStringObjectPreference p = new MapStringObjectPreference(USER, id, defValue, description);
		return regPreference(p);
	}

	// REPLICA
	public final CommonPreference<Integer> OPENDB_REPLICATE_INTERVAL = registerIntPreference(USER, "opendb.replicate.interval", 15, "Time interval to replicate blocks").editable();
	public final CommonPreference<String> OPENDB_REPLICATE_URL = registerStringPreference(USER, "opendb.replicate.url", "https://test.openplacereviews.org/api/", "Main source to replicate blocks").editable();

	// BLOCK AND HISTORY
	public final CommonPreference<Boolean> OPENDB_STORE_HISTORY = registerBooleanPreference("opendb.db.store-history", true, "Store history of operations").editable().restartNeeded();
	public final CommonPreference<Double> OPENDB_COMPACT_COEFICIENT = registerDoublePreference("opendb.db.compactCoefficient", 1.0,  "Compact coefficient for compacting blockchain").editable();
	public final CommonPreference<Integer> OPENDB_SUPERBLOCK_SIZE = registerIntPreference("opendb.db.dbSuperblockSize", 32,  "The amount of blocks to create superblock in a database").editable();

	// LOCAL STORAGE
	public final CommonPreference<String> OPENDB_STORAGE_LOCAL_STORAGE_PATH = registerStringPreference("opendb.storage.local-storage", "", "Path for storing resource files").restartNeeded().editable();
	// store unused resources 24h
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
	public final CommonPreference<Integer> OPENDB_BOTS_MIN_INTERVAL = registerIntPreference("opendb.bots.minInterval", 60, "Min interval to start bots").editable();
	public final CommonPreference<Integer> OPENDB_BOTS_MAX_LOGS_AMOUNT = registerIntPreference("opendb.bots.maxLogsSize", 10, "Max amount logs of bot launches").editable();

	// OBJTABLES SETTINGS
	
	public final CommonPreference<Map<String, Object>> OBJTABLES_LOGINS = registerTableMapping("obj_logins", 2, "sys.login", "sys.signup");
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
