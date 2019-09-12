package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.openplacereviews.opendb.service.BlocksManager.BLOCKCHAIN_SETTINGS;

@Service
public class SettingsManager {

	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private JsonFormatter jsonFormatter;

	public interface OpendbPreference<T> {

		T get();

		boolean set(T obj, Boolean restartIsNeeded);

		String getId();

	}

	private static final String PREFERENCES_NAME = "opendb.settings";

	private Map<String, OpendbPreference<?>> registeredPreferences = new LinkedHashMap<>();

	public boolean savePreferences() {
		return dbSchemaManager.setSetting(jdbcTemplate, PREFERENCES_NAME, jsonFormatter.fullObjectToJson(registeredPreferences));
	}

	public Collection<OpendbPreference<?>> getPreferences() {
		return registeredPreferences.values();
	}

	public OpendbPreference<?> loadPreferenceByKey(String keyPreference) {
		return registeredPreferences.get(keyPreference);
	}

	public List<OpendbPreference<?>> loadContainsPreferencesByKey(String keyPreference) {
		List<OpendbPreference<?>> opendbPreferences = new ArrayList<>();
		for (String key : registeredPreferences.keySet()) {
			if (key.contains(keyPreference)) {
				opendbPreferences.add(registeredPreferences.get(key));
			}
		}

		return opendbPreferences;
	}

	public void initPrefs() {
		String settings = dbSchemaManager.getSetting(jdbcTemplate, PREFERENCES_NAME);
		if (settings != null) {
			for(Map.Entry<String, Map> opendbPreferenceEntry : jsonFormatter.fromJsonToOpendbPreferenceMap(settings).entrySet()) {
				OpendbPreference<?> preference = registeredPreferences.get(opendbPreferenceEntry.getKey());
				Object value = opendbPreferenceEntry.getValue().get("value");
				if (preference == null) {
					value = generateObjectByType(value, (String) opendbPreferenceEntry.getValue().get("type"));
				}
				boolean updated = updatePreference(opendbPreferenceEntry.getKey(), value, (Boolean) opendbPreferenceEntry.getValue().get("restartIsNeeded"), false);
				if (!updated) {
					throw new IllegalArgumentException("Settings was not loaded");
				}
			}
		}
	}

	public Object generateObjectByType(String object, String type) {
		switch (type) {
			case "Boolean": {
				return Boolean.parseBoolean(object);
			}
			case "Long": {
				return Long.parseLong(object);

			}
			case "Integer": {
				return Integer.parseInt(object);

			}
			case "Float": {
				return Float.parseFloat(object);

			}
			case "Map": {
				return jsonFormatter.fromJsonToTreeMap(object);
			}
			default: {
				return object;
			}
		}
	}

	private Object generateObjectByType(Object object, String type) {
		switch (type) {
			case "Boolean": {
				return (Boolean) object;
			}
			case "Long": {
				return ((Number)object).longValue();
			}
			case "Integer": {
				return ((Number)object).intValue();
			}
			case "Float": {
				return ((Number)object).floatValue();
			}
			default: {
				return object;
			}
		}
	}

	public boolean updatePreference(String key, Object value, Boolean restartIsNeeded, boolean saveSettings) {
		OpendbPreference<?> preference = registeredPreferences.get(key);
		if (preference != null) {
			if (preference instanceof BooleanPreference) {
				if (value instanceof Boolean) {
					((BooleanPreference) preference).set((Boolean) value, restartIsNeeded);
					return true;
				}
			} else if (preference instanceof StringPreference) {
				if (value instanceof String) {
					((StringPreference) preference).set((String) value, restartIsNeeded);
					return true;
				}
			} else if (preference instanceof FloatPreference) {
				if (value instanceof Number) {
					((FloatPreference) preference).set(((Number) value).floatValue(), restartIsNeeded);
					return true;
				}
			} else if (preference instanceof IntPreference) {
				if (value instanceof Number) {
					((IntPreference) preference).set(((Number) value).intValue(), restartIsNeeded);
					return true;
				}
			} else if (preference instanceof LongPreference) {
				if (value instanceof Number) {
					((LongPreference) preference).set(((Number) value).longValue(), restartIsNeeded);
					return true;
				}
			} else if (preference instanceof MapStringObjectPreference) {
				if (value instanceof Map) {
					((MapStringObjectPreference) preference).set((Map<String, Object>) value, restartIsNeeded);
					return true;
				}
			}
		} else {
			if (value instanceof Boolean) {
				registerBooleanPreference(key, (Boolean) value, restartIsNeeded, saveSettings);
				return true;
			} else if (value instanceof String) {
				registerStringPreference(key, (String) value, restartIsNeeded, saveSettings);
				return true;
			} else if (value instanceof Float) {
				registerFloatPreference(key, (Float) value, restartIsNeeded, saveSettings);
				return true;
			} else if (value instanceof Integer) {
				registerIntPreference(key, (Integer) value, restartIsNeeded, saveSettings);
				return true;
			} else if (value instanceof Long) {
				registerLongPreference(key, (Long) value, restartIsNeeded, saveSettings);
				return true;
			} else if (value instanceof Map) {
				registerMapPreference(key, (Map<String, Object>) value, restartIsNeeded, saveSettings);
				return true;
			}
		}
		return false;
	}

	/////////////// PREFERENCES classes ////////////////

	public abstract class CommonPreference<T> implements OpendbPreference<T> {
		private final String id;
		private T value;
		private boolean restartIsNeeded;
		private String type;

		public CommonPreference(String id, T value, boolean restartIsNeeded) {
			this.id = id;
			this.value = value;
			this.restartIsNeeded = restartIsNeeded;
			registeredPreferences.put(id, this);
		}

		public Boolean restartIsNeeded() {
			return restartIsNeeded;
		}

		protected T getValue() {
			return value;
		}

		public String getType() {
			return type;
		}

		@Override
		public String getId() {
			return id;
		}

		@Override
		public boolean set(T obj, Boolean restartIsNeeded) {
			if (value.getClass() == obj.getClass()) {
				value = obj;
				this.restartIsNeeded = restartIsNeeded;
				return true;
			}

			return false;
		}

		@Override
		public T get() {
			return value;
		}

		public void setType(String type) {
			this.type = type;
		}
	}

	private class BooleanPreference extends CommonPreference<Boolean> {

		private BooleanPreference(String id, Boolean defaultValue, boolean neededRestart) {
			super(id, defaultValue, neededRestart);
			this.setType("Boolean");
		}
	}

	private class IntPreference extends CommonPreference<Integer> {

		private IntPreference(String id, Integer defaultValue, boolean neededRestart) {
			super(id, defaultValue, neededRestart);
			this.setType("Integer");
		}
	}

	private class LongPreference extends CommonPreference<Long> {

		private LongPreference(String id, Long defaultValue, boolean neededRestart) {
			super(id, defaultValue, neededRestart);
			this.setType("Long");
		}

	}

	private class FloatPreference extends CommonPreference<Float> {

		private FloatPreference(String id, Float defaultValue, boolean neededRestart) {
			super(id, defaultValue, neededRestart);
			this.setType("Float");
		}

	}

	private class StringPreference extends CommonPreference<String> {

		private StringPreference(String id, String defaultValue, boolean neededRestart) {
			super(id, defaultValue, neededRestart);
			this.setType("String");
		}

	}

	// TODO implement map
	public class MapStringObjectPreference extends CommonPreference<Map<String, Object>> {

		private MapStringObjectPreference(String id, Map<String, Object> defaultValue, boolean neededRestart) {
			super(id, defaultValue, neededRestart);
			this.setType("Map");
		}

		public boolean keyExist(String key) {
			Map<String, Object> map = get();
			return map.containsKey(key);
		}

		public void setValue(String key, Object value) {
			Map<String, Object> map = get();
			map.put(key, value);
		}

		public void removeKey(String key) {
			Map<String, Object> map = get();
			map.remove(key);
		}

	}

	public class ListStringPreference extends StringPreference {

		private String delimiter;

		private ListStringPreference(String id, String defaultValue, boolean neededRestart) {
			super(id, defaultValue, neededRestart);
		}

		public boolean addValue(String res) {
			String vl = get();
			if (vl == null || vl.isEmpty()) {
				vl = res + delimiter;
			} else {
				vl = vl + res + delimiter;
			}
			set(vl, this.restartIsNeeded());
			return true;
		}

		public void clearAll() {
			set("", this.restartIsNeeded());
		}

		public boolean containsValue(String res) {
			String vl = get();
			String r = res + delimiter;
			return vl.startsWith(r) || vl.indexOf(delimiter + r) >= 0;
		}

		public boolean removeValue(String res) {
			String vl = get();
			String r = res + delimiter;
			if(vl != null) {
				if(vl.startsWith(r)) {
					vl = vl.substring(r.length());
					set(vl, this.restartIsNeeded());
					return true;
				} else {
					int it = vl.indexOf(delimiter + r);
					if(it >= 0) {
						vl = vl.substring(0, it + delimiter.length()) + vl.substring(it + delimiter.length() + r.length());
					}
					set(vl, this.restartIsNeeded());
					return true;
				}
			}
			return false;
		}


	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Boolean> registerBooleanPreference(String id, boolean defValue, boolean neededRestart, boolean saveSettings) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Boolean>) registeredPreferences.get(id);
		}
		BooleanPreference p = new BooleanPreference(id, defValue, neededRestart);
		registeredPreferences.put(id, p);
		if (saveSettings) {
			savePreferences();
		}
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<String> registerStringPreference(String id, String defValue, boolean neededRestart, boolean saveSettings) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<String>) registeredPreferences.get(id);
		}
		StringPreference p = new StringPreference(id, defValue, neededRestart);
		registeredPreferences.put(id, p);
		if (saveSettings) {
			savePreferences();
		}
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Integer> registerIntPreference(String id, int defValue, boolean neededRestart, boolean saveSettings) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Integer>) registeredPreferences.get(id);
		}
		IntPreference p = new IntPreference(id, defValue, neededRestart);
		registeredPreferences.put(id, p);
		if (saveSettings) {
			savePreferences();
		}
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Long> registerLongPreference(String id, long defValue, boolean neededRestart, boolean saveSettings) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Long>) registeredPreferences.get(id);
		}
		LongPreference p = new LongPreference(id, defValue, neededRestart);
		registeredPreferences.put(id, p);
		if (saveSettings) {
			savePreferences();
		}
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Float> registerFloatPreference(String id, float defValue, boolean neededRestart, boolean saveSettings) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Float>) registeredPreferences.get(id);
		}
		FloatPreference p = new FloatPreference(id, defValue, neededRestart);
		registeredPreferences.put(id, p);
		if (saveSettings) {
			savePreferences();
		}
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Map<String, Object>> registerMapPreference(String id, Map<String, Object> defValue, boolean neededRestart, boolean saveSettings) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Map<String, Object>>) registeredPreferences.get(id);
		}
		MapStringObjectPreference p = new MapStringObjectPreference(id, defValue, neededRestart);
		registeredPreferences.put(id, p);
		if (saveSettings) {
			savePreferences();
		}
		return p;
	}

	// REPLICA
	public final CommonPreference<Integer> OPENDB_REPLICATE_INTERVAL = new IntPreference("opendb.replicate.interval", 15, false);
	public final CommonPreference<String> OPENDB_REPLICATE_URL = new StringPreference("opendb.replicate.url", "https://dev.openplacereviews.org/api/", false);

	// BLOCK AND HISTORY
	public final CommonPreference<Boolean> OPENDB_STORE_HISTORY = new BooleanPreference("opendb.db.store-history", true, true);
	public final CommonPreference<Float> OPENDB_COMPACT_COEFICIENT = new FloatPreference("opendb.db.compactCoefficient", 1.0F, false);
	public final CommonPreference<Integer> OPENDB_SUPERBLOCK_SIZE = new IntPreference("opendb.db.dbSuperblockSize", 32, false);

	// LOCAL STORAGE
	public final CommonPreference<String> OPENDB_STORAGE_LOCAL_STORAGE_PATH = new StringPreference("opendb.storage.local-storage", "", true);
	public final CommonPreference<Integer> OPENDB_STORAGE_TIME_TO_STORE_UNUSED_RESOURCE_SEC = new IntPreference("opendb.storage.timeToStoreUnusedSec", 86400, false);

	// IPFS SETTINGS
	public final CommonPreference<String> OPENDB_STORAGE_IPFS_NODE_HOST = new StringPreference("opendb.storage.ipfs.node.host", "", false);
	public final CommonPreference<Integer> OPENDB_STORAGE_IPFS_NODE_PORT = new IntPreference("opendb.storage.ipfs.node.port", 5001, false);
	public final CommonPreference<Integer> OPENDB_STORAGE_IPFS_NODE_READ_TIMEOUT_MS = new IntPreference("opendb.storage.ipfs.node.readTimeoutMs", 10000, false);

	// FILE BACKUP
	public final CommonPreference<String> OPENDB_FILE_BACKUP_DIRECTORY = new StringPreference("opendb.files-backup.directory", "blocks", true);

	// SCHEDULED SERVICE SETTINGS
	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MIS_SECONDS_INTERVAL = new IntPreference("opendb.block-create.minSecondsInterval", 120, false);
	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MIN_QUEUE_SIZE = new IntPreference("opendb.block-create.minQueueSize", 10, false);
	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MAX_SECONDS_INTERVAL = new IntPreference("opendb.block-create.maxSecondsInterval", 3600, false);
	public final CommonPreference<Float> OPENDB_BLOCK_CREATE_MIN_CAPACITY = new FloatPreference("opendb.block-create.minCapacity", 0.7F, false);

	// BOTS
	public final CommonPreference<Integer> OPENDB_BOTS_MIN_INTERVAL = new IntPreference("opendb.bots.minInterval", 1800, false);

	// OBJTABLES SETTINGS
	public final CommonPreference<Map<String, Object>> OBJTABLES_LOGINS = new MapStringObjectPreference("opendb.db-schema.objtables.obj_logins", getDefaultObjLogins(), true);
	public final CommonPreference<Map<String, Object>> OBJTABLES_GRANTS = new MapStringObjectPreference("opendb.db-schema.objtables.obj_grants", getDefaultObjGrants(), true);
	public final CommonPreference<Map<String, Object>> OBJTABLES_SYSTEM = new MapStringObjectPreference("opendb.db-schema.objtables.obj_system", getDefaultObjSystem(), true);

	// BLOCKCHAIN STATUS
	public final CommonPreference<String> OPENDB_BLOCKCHAIN_STATUS = new StringPreference(BLOCKCHAIN_SETTINGS, BlocksManager.BlockchainMgmtStatus.NONE.name(), false);

	private Map<String, Object> getDefaultObjLogins() {
		Map<String, Object> obj_logins = new TreeMap<>();
		obj_logins.put("types", Arrays.asList("sys.login, sys.signup"));
		obj_logins.put("keysize", 2);
		return obj_logins;
	}

	private Map<String, Object> getDefaultObjGrants() {
		Map<String, Object> obj_logins = new TreeMap<>();
		obj_logins.put("types", Arrays.asList("sys.grant"));
		obj_logins.put("keysize", 2);
		return obj_logins;
	}

	private Map<String, Object> getDefaultObjSystem() {
		Map<String, Object> obj_logins = new TreeMap<>();
		obj_logins.put("types", Arrays.asList("sys.validate", "sys.operation", "sys.role"));
		obj_logins.put("keysize", 1);
		return obj_logins;
	}
}
