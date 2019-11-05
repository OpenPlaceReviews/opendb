package org.openplacereviews.opendb.service.bots;

import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.SettingsManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.openplacereviews.opendb.service.SettingsManager.BOT_INACTIVE_CACHE_SECONDS;

public class PublicDataUpdateBot extends GenericMultiThreadBot<PublicDataUpdateBot> {

	@Autowired
	private PublicDataManager publicDataManager;

	@Autowired
	private SettingsManager settingsManager;

	private SettingsManager.CommonPreference p;
	private int totalCnt = 1;
	private int progress = 0;

	public PublicDataUpdateBot(String id) {
		super(id);
	}

	@Override
	public String getTaskDescription() {
		return "Updating " + id + "-api cache";
	}

	@Override
	public String getTaskName() {
		return "update-" + id + "-api-cache";
	}

	@Override
	public PublicDataUpdateBot call() throws Exception {
		SettingsManager.CommonPreference<Map<String, Object>> botPreference = settingsManager.getPreferenceByKey(SettingsManager.OPENDB_ENDPOINTS_CONFIG.getId(id));
		long storeInactiveCacheSeconds = (long) botPreference.get().get(BOT_INACTIVE_CACHE_SECONDS);
		addNewBotStat();
		info("Task 'Updating API cache' is started");
		try {
			PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint = publicDataManager.getEndpoints().get(id);
			info("Start calculating keys for updating");
			Set<Object> cacheKeys = apiEndpoint.getCacheKeys();
			List<Object> keysForRemoving = new ArrayList<>();
			if (cacheKeys.size() > 0) {
				for (Object key : cacheKeys) {
					PublicDataManager.CacheHolder cacheHolder = apiEndpoint.getCacheHolder(key);
					if (cacheHolder.accessTime - System.currentTimeMillis() / 1000L > storeInactiveCacheSeconds) {
						keysForRemoving.add(key);
					}
				}
				totalCnt = cacheKeys.size();
			}
			info(String.format("%s keys to upgrade, %s of them to be deleted", totalCnt, keysForRemoving.size()));

			info(String.format("Start updating cache for '%s' endpoint...", id));
			for (Object key : cacheKeys) {
				if (keysForRemoving.contains(key)) {
					apiEndpoint.removeCacheHolder(key);
					info("Cache key: " + key + " was removed");
				} else {
					apiEndpoint.updateCacheHolder(key);
					info("Cache key: " + key + " was updated");
				}
				progress++;
			}
			info(String.format("Finish updating cache for '%s' endpoint!", id));

			setSuccessState();
			info("'Updating API cache' is finished");
		} catch (Exception e) {
			setFailedState();
			info("'Updating API cache' has failed: " + e.getMessage(), e);
			throw e;
		} finally {
			super.shutdown();
		}
		return this;
	}

	@Override
	public int total() {
		return totalCnt;
	}

	@Override
	public int progress() {
		return progress;
	}
}
