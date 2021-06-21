package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
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
	
	@Autowired
	protected SettingsManager settingsManager;
	
	@Autowired 
	protected AutowireCapableBeanFactory beanFactory;
	
	@Autowired
	protected BotManager botManager;
	
	private Map<String, PublicAPIEndpoint<?, ?>> endpoints = new ConcurrentHashMap<>(); 
	
	private Map<String, Class<? extends IPublicDataProvider<?, ?>>> dataProviders = new ConcurrentHashMap<>();
	
	
	private int localVersion = 0;

	public void updateEndpoints() {
		updateEndpoints(null);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	public void updateEndpoints(String endpointFilter) {
		int v = SettingsManager.OPENDB_ENDPOINTS_CONFIG.version.get();
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
			PublicAPIEndpoint<?, ?> existingEndpoint = endpoints.get(id);
			String providerDef = pref.getStringValue(ENDPOINT_PROVIDER, null);
			if (existingEndpoint == null || !existingEndpoint.provider.getClass().getName().equals(providerDef)) {
				Class<? extends IPublicDataProvider<?, ?>> providerClass = dataProviders.get(providerDef);
				IPublicDataProvider<?, ?> provider = null;
				if (providerClass != null) {
					try {
						provider = providerClass.newInstance();
						beanFactory.autowireBean(provider);
					} catch (Exception e) {
						LOGGER.error(e.getMessage(), e);
					}
				}
				if (provider == null) {
					LOGGER.error(String.format("Endpoint '%s' has invalid data provider '%s'", id, providerDef));
					continue;
				}

				PublicAPIEndpoint<?, ?> endpoint = new PublicAPIEndpoint(provider, pref);
				endpoints.put(id, endpoint);
			}
		}
		localVersion = v;
	}
	
	
	public boolean operationAdded(OpOperation op, OpBlock block) {
		boolean changed = false;
		for (PublicAPIEndpoint<?, ?> e : endpoints.values()) {
			try {
				boolean updated = updateEndpointWithNewOperation(e, op, block);
				if (updated) {
					changed = true;
					// bot is not needed here cause mostly it will be updated by user-request with invalidate=true
					// botManager.startBot(PublicDataUpdateBot.apiEndpointBotName(e));
				}
			} catch (RuntimeException es) {
				LOGGER.warn(String.format("Error updating endpoint cache '%s': %s", e.getId(), es.getMessage()), es);
			}
		}
		return changed;
	}

	private <T, K> boolean updateEndpointWithNewOperation(PublicAPIEndpoint<T, K> e, OpOperation op, OpBlock block) {
		if (e != null && e.provider != null) {
			return e.provider.operationAdded(e, op, block);
		}
		return false;
	}

	public void registerDataProvider(Class<? extends IPublicDataProvider<?, ?>> provider) {
		dataProviders.put(provider.getName(), provider);
	}
	
	public PublicAPIEndpoint<?, ?> getEndpoint(String path) {
		checkIfApiUpdateNeeded();
		return endpoints.get(path);
	}
	
	public PublicAPIEndpoint<?, ?> getEndpointById(String id) {
		checkIfApiUpdateNeeded();
		return endpoints.get(id);
	}
	
	private void checkIfApiUpdateNeeded() {
		if(localVersion != SettingsManager.OPENDB_ENDPOINTS_CONFIG.version.get()) {
			updateEndpoints();
		}
	}

	public Map<String, PublicAPIEndpoint<?, ?>> getEndpoints() {
		checkIfApiUpdateNeeded();
		return endpoints;
	}
	
	public static class CacheHolder<T> {
		public long evalTime;
		public long accessTime;
		public long access;
		public long size;
		public boolean forceUpdate;
		transient T value;
	}
	
	public static class PublicAPIEndpoint<P, T> {

		protected transient IPublicDataProvider<P, T> provider;
		protected transient MapStringObjectPreference map;
		
		protected final String path;
		protected final String id;
		protected Map<P, CacheHolder<T>> cacheObjects = new ConcurrentHashMap<P, CacheHolder<T>>();
		protected boolean cacheDisabled;
		private PerformanceMetric dataMetric;
		private PerformanceMetric pageMetric;
		private PerformanceMetric reqMetric;

		public PublicAPIEndpoint(IPublicDataProvider<P, T> provider, MapStringObjectPreference map) {
			this.provider = provider;
			this.map = map;
			this.path = (String) map.get().get(ENDPOINT_PATH);
			this.id = (String) map.get().get(SettingsManager.ENDPOINT_ID);
			cacheDisabled = map.getLong(CACHE_TIME_SEC, DEFAULT_CACHE_TIME_SECONDS) <= 0;
			dataMetric = PerformanceMetrics.i().getMetric("public.data", path);
			reqMetric = PerformanceMetrics.i().getMetric("public.req", path);
			pageMetric = PerformanceMetrics.i().getMetric("public.page", path);
		}
		
		public String getPath() {
			return path;
		}
		
		public String getId() {
			return id;
		}
		
		public List<P> retrieveKeysToReevaluate() {
			return provider.getKeysToCache(this);
		}

		public Set<P> getCacheKeys() {
			return cacheObjects.keySet();
		}

		public CacheHolder<T> getCacheHolder(P key) {
			return cacheObjects.get(key);
		}

		public void removeCacheHolder(P key) {
			cacheObjects.remove(key);
		}

		public void updateCacheHolder(P key) {
			evalCacheValue(getNow(), key);
		}

		public long getNow() {
			return System.currentTimeMillis() / 1000L;
		}

		public AbstractResource getContent(Map<String, String[]> params) {
			Metric m = reqMetric.start();
			try {
				CacheHolder<T> ch = getCacheHolder(params);
				return provider.formatContent(ch.value);
			} finally {
				m.capture();
			}
		}
		
		public T getContentObject(Map<String, String[]> params) {
			Metric m = reqMetric.start();
			try {
				CacheHolder<T> ch = getCacheHolder(params);
				return ch.value;
			} finally {
				m.capture();
			}
		}

		private CacheHolder<T> getCacheHolder(Map<String, String[]> params) {
			long now = getNow();
			P p = provider.formatParams(params);
			CacheHolder<T> ch = null;
			if (!cacheDisabled) {
				ch = cacheObjects.get(p);
				if (ch != null) {
					ch.accessTime = now;
					ch.access++;
					long timePast = now - ch.evalTime;
					long intWait = map.getLong(CACHE_TIME_SEC, DEFAULT_CACHE_TIME_SECONDS);
					if (timePast > intWait || ch.forceUpdate) {
						ch = null;
					}
				}
			}
			if (ch == null) {
				ch = evalCacheValue(now, p);
			}
			return ch;
		}

		private CacheHolder<T> evalCacheValue(long now, P p) {
			CacheHolder<T> ch;
			Metric mt = dataMetric.start();
			ch = new CacheHolder<>();
			ch.value = provider.getContent(p);
			if (!cacheDisabled) {
				ch.accessTime = now;
				ch.evalTime = now;
				String serializeValue = provider.serializeValue(ch.value);
				if (serializeValue != null) {
					ch.size = serializeValue.length();
				}
				cacheObjects.put(p, ch);
			}
			mt.capture();
			return ch;
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

	public int getVersion() {
		return localVersion;
	}
	

}
