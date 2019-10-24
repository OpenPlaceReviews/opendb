package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.PerformanceMetrics;
import org.openplacereviews.opendb.ops.PerformanceMetrics.Metric;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.service.SettingsManager.MapStringObjectPreference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.core.io.AbstractResource;
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
	
	private Map<String, PublicAPIEndpoint<?>> endpoints = new ConcurrentHashMap<>(); 
	
	private Map<String, Class<? extends IPublicDataProvider<?>>> dataProviders = new ConcurrentHashMap<>();

	public void updateEndpoints() {
		updateEndpoints(null);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
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
			Class<? extends IPublicDataProvider<?>> providerClass = dataProviders.get(providerDef);
			IPublicDataProvider<?> provider = null;
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
			
			PublicAPIEndpoint<?> endpoint = new PublicAPIEndpoint(provider, pref.get());
			endpoints.put(id, endpoint);
		}
	}

	public void registerDataProvider(Class<? extends IPublicDataProvider<?>> provider) {
		dataProviders.put(provider.getName(), provider);
	}
	
	
	public PublicAPIEndpoint<?> getEndpoint(String path) {
		return endpoints.get(path);
	}
	
	public Map<String, PublicAPIEndpoint<?>> getEndpoints() {
		return endpoints;
	}
	
	public static class PublicAPIEndpoint<T> {

		private IPublicDataProvider<T> provider;
		private String path;
		private PerformanceMetric dataMetric;
		private PerformanceMetric pageMetric;

		public PublicAPIEndpoint(IPublicDataProvider<T> provider, Map<String, Object> map) {
			this.provider = provider;
			this.path = (String) map.get(ENDPOINT_PATH);
			dataMetric = PerformanceMetrics.i().getMetric("public.data", path);
			pageMetric = PerformanceMetrics.i().getMetric("public.page", path);
		}
		
		
		public String getPath() {
			return path;
		}
		
		public AbstractResource getContent(Map<String, String[]> params) {
			Metric m = dataMetric.start();
			try {
				T content = provider.getContent(params);
				return provider.formatContent(content);
			} finally {
				m.capture();
			}
		}
		
		public AbstractResource getPage(Map<String, String[]> params) {
			Metric m = pageMetric.start();
			try {
				return provider.getPage(params);
			} finally {
				m.capture();
			}
		}
		
	}
	

}
