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

	@Autowired
	private DBSchemaManager dbSchemaManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private JsonFormatter jsonFormatter;

	public interface OpendbPreference<T> {

		T get();

		boolean isCanEdit();

		boolean set(T obj);

		String getId();

	}

	private static final String PREFERENCES_NAME = "opendb.settings";

	private Map<String, OpendbPreference<?>> registeredPreferences = new LinkedHashMap<>();

	public boolean savePreferences() {
		Map<String, OpendbPreference<?>> mapForStoring = new HashMap<>();
		for (Map.Entry<String, OpendbPreference<?>> entry : registeredPreferences.entrySet()) {
			if (entry.getValue().isCanEdit()) {
				mapForStoring.put(entry.getKey(), entry.getValue());
			}
		}
		return dbSchemaManager.setSetting(jdbcTemplate, PREFERENCES_NAME, jsonFormatter.fullObjectToJson(mapForStoring));
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

	@PostConstruct
	protected void initPrefs() {
		String settings = dbSchemaManager.getSetting(jdbcTemplate, PREFERENCES_NAME);
		if (settings != null) {
			for(Map.Entry<String, Map> opendbPreferenceEntry : jsonFormatter.fromJsonToOpendbPreferenceMap(settings).entrySet()) {
				OpendbPreference<?> preference = registeredPreferences.get(opendbPreferenceEntry.getKey());
				if (preference != null && preference.isCanEdit()) {
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

	public boolean updatePreference(String key, Object value) {
		OpendbPreference<?> preference = registeredPreferences.get(key);
		if (preference != null && preference.isCanEdit()) {
			if (preference instanceof BooleanPreference) {
				if (value instanceof Boolean) {
					((BooleanPreference) preference).set((Boolean) value);
					savePreferences();
					return true;
				}
			} else if (preference instanceof StringPreference) {
				if (value instanceof String) {
					((StringPreference) preference).set((String) value);
					savePreferences();
					return true;
				}
			} else if (preference instanceof FloatPreference) {
				if (value instanceof Number) {
					((FloatPreference) preference).set(((Number) value).floatValue());
					savePreferences();
					return true;
				}
			} else if (preference instanceof IntPreference) {
				if (value instanceof Number) {
					((IntPreference) preference).set(((Number) value).intValue());
					savePreferences();
					return true;
				}
			} else if (preference instanceof LongPreference) {
				if (value instanceof Number) {
					((LongPreference) preference).set(((Number) value).longValue());
					savePreferences();
					return true;
				}
			} else if (preference instanceof MapStringObjectPreference) {
				if (value instanceof Map) {
					((MapStringObjectPreference) preference).set((Map<String, Object>) value);
					savePreferences();
					return true;
				}
			}
		}
		return false;
	}

	/////////////// PREFERENCES classes ////////////////

	public abstract class CommonPreference<T> implements OpendbPreference<T> {
		private final String id;
		protected T value;
		private boolean restartIsNeeded;
		private String description;
		private boolean canEdit;
		private String type;

		public CommonPreference(String id, T value, boolean restartIsNeeded, String description, boolean canEdit) {
			this.id = id;
			this.value = value;
			this.restartIsNeeded = restartIsNeeded;
			this.description = description;
			this.canEdit = canEdit;
			registeredPreferences.put(id, this);
		}

		public String getDescription() {
			return description;
		}

		@Override
		public boolean isCanEdit() {
			return canEdit;
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
		public boolean set(T obj) {
			if (value.getClass() == obj.getClass()) {
				value = obj;
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

		private BooleanPreference(String id, Boolean defaultValue, boolean neededRestart, String description, boolean canEdit) {
			super(id, defaultValue, neededRestart, description, canEdit);
			this.setType("Boolean");
		}
	}

	private class IntPreference extends CommonPreference<Integer> {

		private IntPreference(String id, Integer defaultValue, boolean neededRestart, String description, boolean canEdit) {
			super(id, defaultValue, neededRestart, description, canEdit);
			this.setType("Integer");
		}
	}

	private class LongPreference extends CommonPreference<Long> {

		private LongPreference(String id, Long defaultValue, boolean neededRestart, String description, boolean canEdit) {
			super(id, defaultValue, neededRestart, description, canEdit);
			this.setType("Long");
		}

	}

	private class FloatPreference extends CommonPreference<Float> {

		private FloatPreference(String id, Float defaultValue, boolean neededRestart, String description, boolean canEdit) {
			super(id, defaultValue, neededRestart, description, canEdit);
			this.setType("Float");
		}

	}

	private class StringPreference extends CommonPreference<String> {

		private StringPreference(String id, String defaultValue, boolean neededRestart, String description, boolean canEdit) {
			super(id, defaultValue, neededRestart, description, canEdit);
			this.setType("String");
		}

	}

	public class MapStringObjectPreference extends CommonPreference<Map<String, Object>> {

		private MapStringObjectPreference(String id, Map<String, Object> defaultValue, boolean neededRestart, String description, boolean canEdit) {
			super(id, defaultValue, neededRestart, description, canEdit);
			this.setType("Map");
		}

		public boolean keyExist(String key) {
			Map<String, Object> map = get();
			return map.containsKey(key);
		}

		@Override
		public boolean set(Map<String, Object> obj) {
			if (obj != null) {
				this.value = obj;
				return false;
			}

			return false;
		}

		public void set(String key, Object value) {
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

		private ListStringPreference(String id, String defaultValue, boolean neededRestart, String description, boolean canEdit) {
			super(id, defaultValue, neededRestart, description, canEdit);
		}

		public boolean addValue(String res) {
			String vl = get();
			if (vl == null || vl.isEmpty()) {
				vl = res + delimiter;
			} else {
				vl = vl + res + delimiter;
			}
			set(vl);
			return true;
		}

		public void clearAll() {
			set("");
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
					set(vl);
					return true;
				} else {
					int it = vl.indexOf(delimiter + r);
					if(it >= 0) {
						vl = vl.substring(0, it + delimiter.length()) + vl.substring(it + delimiter.length() + r.length());
					}
					set(vl);
					return true;
				}
			}
			return false;
		}


	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Boolean> registerBooleanPreference(String id, boolean defValue, boolean neededRestart, String description, boolean canEdit) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Boolean>) registeredPreferences.get(id);
		}
		BooleanPreference p = new BooleanPreference(id, defValue, neededRestart, description, canEdit);
		registeredPreferences.put(id, p);
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<String> registerStringPreference(String id, String defValue, boolean neededRestart, String description, boolean canEdit) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<String>) registeredPreferences.get(id);
		}
		StringPreference p = new StringPreference(id, defValue, neededRestart, description, canEdit);
		registeredPreferences.put(id, p);
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Integer> registerIntPreference(String id, int defValue, boolean neededRestart, String description, boolean canEdit) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Integer>) registeredPreferences.get(id);
		}
		IntPreference p = new IntPreference(id, defValue, neededRestart, description, canEdit);
		registeredPreferences.put(id, p);
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Long> registerLongPreference(String id, long defValue, boolean neededRestart, String description, boolean canEdit) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Long>) registeredPreferences.get(id);
		}
		LongPreference p = new LongPreference(id, defValue, neededRestart, description, canEdit);
		registeredPreferences.put(id, p);
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Float> registerFloatPreference(String id, float defValue, boolean neededRestart, String description, boolean canEdit) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Float>) registeredPreferences.get(id);
		}
		FloatPreference p = new FloatPreference(id, defValue, neededRestart, description, canEdit);
		registeredPreferences.put(id, p);
		return p;
	}

	@SuppressWarnings("unchecked")
	public CommonPreference<Map<String, Object>> registerMapPreference(String id, Map<String, Object> defValue, boolean neededRestart, String description, boolean canEdit) {
		if (registeredPreferences.containsKey(id)) {
			return (CommonPreference<Map<String, Object>>) registeredPreferences.get(id);
		}
		MapStringObjectPreference p = new MapStringObjectPreference(id, defValue, neededRestart, description, canEdit);
		registeredPreferences.put(id, p);
		return p;
	}

	// REPLICA
	public final CommonPreference<Integer> OPENDB_REPLICATE_INTERVAL = new IntPreference("opendb.replicate.interval", 15, false, "Determines the replication block interval", true);
	public final CommonPreference<String> OPENDB_REPLICATE_URL = new StringPreference("opendb.replicate.url", "https://dev.openplacereviews.org/api/", false, "Determines the source from which will be replicated blocks", false);

	// BLOCK AND HISTORY
	public final CommonPreference<Boolean> OPENDB_STORE_HISTORY = new BooleanPreference("opendb.db.store-history", true, true, "Determines to store history or not", true);
	public final CommonPreference<Float> OPENDB_COMPACT_COEFICIENT = new FloatPreference("opendb.db.compactCoefficient", 1.0F, false, "Determines compact coefficient for compacting", true);
	public final CommonPreference<Integer> OPENDB_SUPERBLOCK_SIZE = new IntPreference("opendb.db.dbSuperblockSize", 32, false, "Determines the amount of blocks for creating superblock", true);

	// LOCAL STORAGE
	public final CommonPreference<String> OPENDB_STORAGE_LOCAL_STORAGE_PATH = new StringPreference("opendb.storage.local-storage", "", true, "Determines path for storing resource files", true);
	public final CommonPreference<Integer> OPENDB_STORAGE_TIME_TO_STORE_UNUSED_RESOURCE_SEC = new IntPreference("opendb.storage.timeToStoreUnusedSec", 86400, false, "Determines a max time for storing unused resources", true);

	// IPFS SETTINGS
	public final CommonPreference<String> OPENDB_STORAGE_IPFS_NODE_HOST = new StringPreference("opendb.storage.ipfs.node.host", "", false, "Determines hostname to ipfs node", true);
	public final CommonPreference<Integer> OPENDB_STORAGE_IPFS_NODE_PORT = new IntPreference("opendb.storage.ipfs.node.port", 5001, false, "Determines hostname to ipfs port", true);
	public final CommonPreference<Integer> OPENDB_STORAGE_IPFS_NODE_READ_TIMEOUT_MS = new IntPreference("opendb.storage.ipfs.node.readTimeoutMs", 10000, false, "Determines a max time for reading from node", true);

	// FILE BACKUP
	public final CommonPreference<String> OPENDB_FILE_BACKUP_DIRECTORY = new StringPreference("opendb.files-backup.directory", "blocks", true, "Determines path for storing block backup", true);

	// SCHEDULED SERVICE SETTINGS
	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MIS_SECONDS_INTERVAL = new IntPreference("opendb.block-create.minSecondsInterval", 120, false, "Determines a min interval between creating blocks", true);
//	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MIN_QUEUE_SIZE = new IntPreference("opendb.block-create.minQueueSize", 10, false, "", true);
	public final CommonPreference<Integer> OPENDB_BLOCK_CREATE_MAX_SECONDS_INTERVAL = new IntPreference("opendb.block-create.maxSecondsInterval", 3600, false, "Determines a max interval between creating blocks", true);
	public final CommonPreference<Float> OPENDB_BLOCK_CREATE_MIN_CAPACITY = new FloatPreference("opendb.block-create.minCapacity", 0.7F, false, "Determines min capacity for blocks", true);

	// BOTS
	public final CommonPreference<Integer> OPENDB_BOTS_MIN_INTERVAL = new IntPreference("opendb.bots.minInterval", 1800, false, "Determines interval for launches bots", true);

	// OBJTABLES SETTINGS
	public final CommonPreference<Map<String, Object>> OBJTABLES_LOGINS = new MapStringObjectPreference("opendb.db-schema.objtables.obj_logins", getDefaultObjLogins(), true, "Determines table for storing superblock logins objects", false);
	public final CommonPreference<Map<String, Object>> OBJTABLES_GRANTS = new MapStringObjectPreference("opendb.db-schema.objtables.obj_grants", getDefaultObjGrants(), true, "Determines table for storing superblock grants objects", false);
	public final CommonPreference<Map<String, Object>> OBJTABLES_SYSTEM = new MapStringObjectPreference("opendb.db-schema.objtables.obj_system", getDefaultObjSystem(), true, "Determines table for storing superblock system objects", false);

	// BLOCKCHAIN STATUS
	public final CommonPreference<String> OPENDB_BLOCKCHAIN_STATUS = new StringPreference(BLOCKCHAIN_SETTINGS, BlocksManager.BlockchainMgmtStatus.NONE.name(), false, "Determines Blockchain status", false);

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
