package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.service.SettingsManager.MapStringObjectPreference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class BotManager {

	private static final Log LOGGER = LogFactory.getLog(BotManager.class);

	@Autowired
	private BlocksManager blocksManager;

	@Autowired 
	private AutowireCapableBeanFactory beanFactory;
	
	@Autowired 
	private SettingsManager settings;

	private Map<String, IOpenDBBot<?>> bots = new TreeMap<String, IOpenDBBot<?>>();

	List<Future<?>> futures = new ArrayList<>();
	ExecutorService service = Executors.newFixedThreadPool(5);

	@SuppressWarnings("unchecked")
	public Map<String, IOpenDBBot<?>> getBots() {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		req.requestCache = true;
		OpBlockChain blc = blocksManager.getBlockchain();
		blc.fetchAllObjects(OpBlockchainRules.OP_BOT, req);
		if (req.cacheObject != null) {
			return (Map<String, IOpenDBBot<?>>) req.cacheObject;
		}
		return recreateBots(req, blc);
	}


	private synchronized Map<String, IOpenDBBot<?>> recreateBots(OpBlockChain.ObjectsSearchRequest req, OpBlockChain blc) {
		Map<String, IOpenDBBot<?>> nbots = new TreeMap<>(this.bots);
		for (OpObject cfg : req.result) {
			String id = cfg.getId().get(0);
			String api = cfg.getStringValue("api");
			IOpenDBBot<?> exBot = nbots.get(id);
			CommonPreference<Map<String, Object>> p = settings.getPreferenceByKey(SettingsManager.OPENDB_BOTS_CONFIG.getId(id));
			if(p == null) {
				TreeMap<String, Object> mp = new TreeMap<>();
				mp.put(SettingsManager.BOT_ID, id);
				mp.put(SettingsManager.BOT_ENABLED, false);
				p = settings.registerMapPreferenceForFamily(SettingsManager.OPENDB_BOTS_CONFIG, mp);
			}
			if (exBot == null || !exBot.getAPI().equals(api)) {
				try {
					Class<?> bot = Class.forName(api);
					Constructor<?> constructor = bot.getConstructor(OpObject.class);
					IOpenDBBot<?> bi = (IOpenDBBot<?>) constructor.newInstance(cfg);
					nbots.put(id, bi);
					beanFactory.autowireBean(bi);
				} catch (Exception e) {
					LOGGER.error(String.format("Error while creating bot %s instance api %s", id, api), e);
				}
			}
			
		}
		this.bots = nbots;
		blc.setCacheAfterSearch(req, nbots);
		return nbots;
	}

	
	public boolean startBot(String botId) {
		IOpenDBBot<?> botObj = getBots().get(botId);
		MapStringObjectPreference p = getBotConfiguration(botId);
		if (botObj == null || p == null) {
			return false;
		}
		p.setValue(SettingsManager.BOT_LAST_RUN, System.currentTimeMillis() / 1000, true);
		futures.add(service.submit(botObj));
		return true;
	}
	
	public boolean stopBot(String botId) {
		IOpenDBBot<?>  botObj = getBots().get(botId);
		if (botObj == null) {
			return false;
		}
		return botObj.interrupt();
	}
	
	
	public MapStringObjectPreference getBotConfiguration(String botId) {
		CommonPreference<Map<String, Object>> p = settings.getPreferenceByKey(SettingsManager.OPENDB_BOTS_CONFIG.getId(botId));
		return (MapStringObjectPreference) p;
	}
	
	
	public boolean enableBot(String botId, int intervalSeconds) {
		MapStringObjectPreference p = getBotConfiguration(botId);
		if (p == null) {
			return false;
		}
		p.setValue(SettingsManager.BOT_INTERVAL_SECONDS, intervalSeconds, false)
		 .setValue(SettingsManager.BOT_ENABLED, true, true);
		return true;
	}
	
	public boolean disableBot(String botId) {
		MapStringObjectPreference p = getBotConfiguration(botId);
		if (p == null) {
			return false;
		}
		p.setValue(SettingsManager.BOT_ENABLED, false, true);
		return true;
	}


	public void runBotsBySchedule() {
		Map<String, IOpenDBBot<?>> bs = this.bots;
		long now = System.currentTimeMillis() / 1000;
		for(String bid : bs.keySet()) {
			MapStringObjectPreference p = getBotConfiguration(bid);
			if(p.getBoolean(SettingsManager.BOT_ENABLED, false)) {
				long lastRun = p.getLong(SettingsManager.BOT_LAST_RUN, 0);
				long l = p.getLong(SettingsManager.BOT_INTERVAL_SECONDS, 0);
				if(now - lastRun > l) {
					startBot(bid);
				}
			}
		}
	}
	
	
}
