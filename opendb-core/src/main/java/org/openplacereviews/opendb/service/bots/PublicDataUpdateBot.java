package org.openplacereviews.opendb.service.bots;

import org.openplacereviews.opendb.service.GenericMultiThreadBot;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Set;

public class PublicDataUpdateBot extends GenericMultiThreadBot<PublicDataUpdateBot> {

	@Autowired
	private PublicDataManager publicDataManager;

	public PublicDataUpdateBot(String id) {
		super(id);
	}

	@Override
	public String getTaskDescription() {
		return "Updating API cache";
	}

	@Override
	public String getTaskName() {
		return "update-api-cache";
	}

	@Override
	public PublicDataUpdateBot call() throws Exception {
		addNewBotStat();
		info("Task 'Updating API cache' is started");
		try {
			Map<String, PublicDataManager.PublicAPIEndpoint<?, ?>> apiEndpointMap = publicDataManager.getEndpoints();
			for (Map.Entry<String, PublicDataManager.PublicAPIEndpoint<?,?>> entry : apiEndpointMap.entrySet()) {
				info(String.format("Start updating cache for '%s' endpoint...", entry.getKey()));

				PublicDataManager.PublicAPIEndpoint<?, ?> apiEndpoint = entry.getValue();
				Set<Object> keys = apiEndpoint.getCacheKeys();
				info("Updated keys: " + keys.toString());

				info(String.format("Finish updating cache for '%s' endpoint!", entry.getKey()));
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
}
