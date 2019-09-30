package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.springframework.beans.factory.annotation.Autowired;

public class PublicDataManager {
	
	public static final String ENDPOINT_PATH = "path";
	
	@Autowired
	private SettingsManager settingsManager;
	
	private Map<String, PublicAPIEndpoint> endpoints = new ConcurrentHashMap<>(); 
	
	private Map<String, PublicDataProvider> dataProviders = new ConcurrentHashMap<>();

	public void updateEndpoints() {
		updateEndpoints(null);
	}
	public void updateEndpoints(String endpointFilter) {
		List<CommonPreference<Map<String, Object>>> prefs = settingsManager.getPreferencesByPrefix(SettingsManager.OPENDB_ENDPOINTS_CONFIG);
		for(CommonPreference<Map<String, Object>> pref : prefs) {
			Map<String, Object> mp = pref.getValue();
			String id = (String) mp.get(SettingsManager.ENDPOINT_ID);
			if(endpointFilter != null && !endpointFilter.equals(id)) {
				continue;
			}
			// TODO
		}
	}

	public void registerDataProvider(PublicDataProvider provider) {
		dataProviders.put(provider.getClass().getName(), provider);
	}
	
	
	public Map<String, PublicAPIEndpoint> getEndpoints() {
		return endpoints;
	}
	
	public static class PublicAPIEndpoint {
		
	}
	
	public static interface PublicDataProvider {
		
	}
	

}
