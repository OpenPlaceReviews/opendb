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
	public static final String CACHE_TIME_SEC = "cache_time_sec";
	public static final int DEFAULT_CACHE_TIME_SECONDS = 3600;
	public static final String ENDPOINT_PROVIDER = "provider";
	protected static final Log LOGGER = LogFactory.getLog(PublicDataManager.class);
	
	// TODO refresh endpoints on setting change
	// TODO reevaulate cache with bot each (automatically register bot)
	// TODO optimistic lock in multithread to evaluate cache value
	// TODO evaluate cache by specific keys
	// TODO clean up not accessing data for long time
	// TODO public api to check changes on blockchain and display on map
	// TODO UI display (public end point list, cache size, requests count)
	
	@Autowired
	private SettingsManager settingsManager;
	
	@Autowired 
	private AutowireCapableBeanFactory beanFactory;
	
	private Map<String, PublicAPIEndpoint<?, ?>> endpoints = new ConcurrentHashMap<>(); 
	
	private Map<String, Class<? extends IPublicDataProvider<?, ?>>> dataProviders = new ConcurrentHashMap<>();

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
			Class<? extends IPublicDataProvider<?, ?>> providerClass = dataProviders.get(providerDef);
			IPublicDataProvider<?, ?> provider = null;
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
			
			PublicAPIEndpoint<?, ?> endpoint = new PublicAPIEndpoint(provider, pref);
			endpoints.put(id, endpoint);
		}
	}

	public void registerDataProvider(Class<? extends IPublicDataProvider<?, ?>> provider) {
		dataProviders.put(provider.getName(), provider);
	}
	
	
	public PublicAPIEndpoint<?, ?> getEndpoint(String path) {
		return endpoints.get(path);
	}
	
	public Map<String, PublicAPIEndpoint<?, ?>> getEndpoints() {
		return endpoints;
	}
	
	protected static class CacheHolder<T> {
		long evalTime;
		long accessTime;
		long access;
		T value;
	}
	
	public static class PublicAPIEndpoint<P, T> {

		private IPublicDataProvider<P, T> provider;
		private String path;
		private PerformanceMetric dataMetric;
		private PerformanceMetric pageMetric;
		private PerformanceMetric reqMetric;
		private Map<Object, CacheHolder<T>> cacheObjects = new ConcurrentHashMap<Object, CacheHolder<T>>();
		private MapStringObjectPreference map;
		private boolean cacheDisabled;

		public PublicAPIEndpoint(IPublicDataProvider<P, T> provider, MapStringObjectPreference map) {
			this.provider = provider;
			this.map = map;
			this.path = (String) map.get().get(ENDPOINT_PATH);
			cacheDisabled = map.getLong(CACHE_TIME_SEC, DEFAULT_CACHE_TIME_SECONDS) <= 0;
			dataMetric = PerformanceMetrics.i().getMetric("public.data", path);
			reqMetric = PerformanceMetrics.i().getMetric("public.req", path);
			pageMetric = PerformanceMetrics.i().getMetric("public.page", path);
		}
		
		
		public String getPath() {
			return path;
		}
		
		public AbstractResource getContent(Map<String, String[]> params) {
			Metric m = reqMetric.start();
			long now = (System.currentTimeMillis() / 1000l);
			try {
				P p = provider.formatParams(params);
				CacheHolder<T> ch = null;
				if (!cacheDisabled) {
					ch = cacheObjects.get(p);
					if (ch != null) {
						ch.accessTime = now;
						ch.access++;
						long timePast = now - ch.evalTime;
						long intWait = map.getLong(CACHE_TIME_SEC, DEFAULT_CACHE_TIME_SECONDS);
						if (timePast > intWait) {
							ch = null;
						}
					}
				}
				if (ch == null) {
					Metric mt = dataMetric.start();
					ch = new CacheHolder<>();
					if(!cacheDisabled) {
						ch.accessTime = now;
						ch.evalTime = now;
						cacheObjects.put(p, ch);
					}
					ch.value = provider.getContent(p);
					mt.capture();
				}
				return provider.formatContent(ch.value);
			} finally {
				m.capture();
			}
		}
		
		public AbstractResource getPage(Map<String, String[]> params) {
			Metric m = pageMetric.start();
			try {
				return provider.getMetaPage(params);
			} finally {
				m.capture();
			}
		}
		
	}
	

}
