package org.openplacereviews.opendb.service.bots;

import java.util.List;

import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;


public class PublicDataUpdateBot<P, V> extends GenericMultiThreadBot<PublicDataUpdateBot<P, V>> {

	private int totalCnt = 1;
	private int progress = 0;
	private PublicAPIEndpoint<P, V> apiEndpoint;

	public PublicDataUpdateBot(PublicDataManager.PublicAPIEndpoint<P, V> apiEndpoint) {
		super("update-api-cache-" + apiEndpoint.getId());
		this.apiEndpoint = apiEndpoint;
	}

	@Override
	public String getTaskDescription() {
		return "Updating cache for API " + apiEndpoint.getId();
	}

	@Override
	public String getTaskName() {
		return id;
	}

	@Override
	public PublicDataUpdateBot<P, V> call() throws Exception {
		totalCnt = 1;
		progress = 0;
		addNewBotStat();
		info("Task 'Updating API cache' is started");
		try {
			List<P> cacheKeys = apiEndpoint.retrieveKeysToReevaluate();
			if (cacheKeys != null) {
				totalCnt = cacheKeys.size();
				info(String.format("%s keys to update", totalCnt));

				info(String.format("Start updating cache for '%s' endpoint...", id));
				for (P key : cacheKeys) {
					apiEndpoint.updateCacheHolder(key);
					progress++;
				}
				info(String.format("Finish updating cache for '%s' endpoint!", id));
			}
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
