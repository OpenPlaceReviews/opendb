package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.service.SettingsManager.MapStringObjectPreference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Service;

@Service
public class PublicDataManager {
	
	public static final String ENDPOINT_PATH = "path";
	public static final String ENDPOINT_PROVIDER = "provider";
	protected static final Log LOGGER = LogFactory.getLog(PublicDataManager.class);
	
	@Autowired
	private SettingsManager settingsManager;
	
	@Autowired 
	private AutowireCapableBeanFactory beanFactory;
	
	private Map<String, PublicAPIEndpoint> endpoints = new ConcurrentHashMap<>(); 
	
	private Map<String, Class<? extends PublicDataProvider>> dataProviders = new ConcurrentHashMap<>();

	public void updateEndpoints() {
		updateEndpoints(null);
	}
	
	
	public void updateEndpoints(String endpointFilter) {
   		List<CommonPreference<Map<String, Object>>> prefs = settingsManager.getPreferencesByPrefix(SettingsManager.OPENDB_ENDPOINTS_CONFIG);
		for(CommonPreference<Map<String, Object>> cpref : prefs) {
			MapStringObjectPreference pref = (MapStringObjectPreference) cpref;
			String id = pref.getStringValue(SettingsManager.ENDPOINT_ID, null);
			if(id == null) {
				LOGGER.error("Endpoint id is not specified for: " + pref.get());
				continue;
			}
			if(endpointFilter != null && !endpointFilter.equals(id)) {
				continue;
			}
			String providerDef = pref.getStringValue(ENDPOINT_PROVIDER, null);
			Class<? extends PublicDataProvider> providerClass = dataProviders.get(providerDef);
			PublicDataProvider provider = null;
			if(providerClass != null) {
				try {
					provider = providerClass.newInstance();
					beanFactory.autowireBean(provider);
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
			if(provider == null) {
				LOGGER.error(String.format("Endpoint '%s' has invalid data provider '%s'", id, providerDef));
				continue;
			}
			PublicAPIEndpoint endpoint = new PublicAPIEndpoint(provider, pref.get());
			endpoints.put(id, endpoint);
		}
	}

	public void registerDataProvider(Class<? extends PublicDataProvider> provider) {
		dataProviders.put(provider.getName(), provider);
	}
	
	
	public PublicAPIEndpoint getEndpoint(String path) {
		return endpoints.get(path);
	}
	
	public Map<String, PublicAPIEndpoint> getEndpoints() {
		return endpoints;
	}
	
	public static class PublicAPIEndpoint {

		private PublicDataProvider provider;
		private String path;

		public PublicAPIEndpoint(PublicDataProvider provider, Map<String, Object> map) {
			this.provider = provider;
			this.path = (String) map.get(ENDPOINT_PATH);
		}
		
		
		public String getPath() {
			return path;
		}
		
		public String getContent() {
			return provider.getContent();
		}
		
	}
	
	public static interface PublicDataProvider {

		String getContent();
		
	}
	

}
