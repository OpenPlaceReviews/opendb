package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.service.SettingsManager.MapStringObjectPreference;
import org.openplacereviews.opendb.service.bots.PublicDataUpdateBot;
import org.openplacereviews.opendb.service.bots.UpdateIndexesBot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.openplacereviews.opendb.service.SettingsManager.*;

@Service
public class BotManager {

	private static final Log LOGGER = LogFactory.getLog(BotManager.class);

	@Autowired
	private BlocksManager blocksManager;

	@Autowired 
	private AutowireCapableBeanFactory beanFactory;
	
	@Autowired 
	private SettingsManager settings;
	
	@Autowired
	private PublicDataManager publicDataManager;

	private Map<String, IOpenDBBot<?>> bots = new TreeMap<String, IOpenDBBot<?>>();
	private Map<String, IOpenDBBot<?>> systemBots = new TreeMap<String, IOpenDBBot<?>>();
	private List<Future<?>> futures = new ArrayList<>();
	private ExecutorService service = Executors.newFixedThreadPool(5);
	

	@PostConstruct
	public void initSystemBots() {
		regSystemBot(new UpdateIndexesBot("update-indexes"), systemBots);
	}

	public void regSystemBot(IOpenDBBot<?> bt, Map<String, IOpenDBBot<?>> bots) {
		beanFactory.autowireBean(bt);
		bots.put(bt.getId(), bt);
	}

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
			recreateBotInstance(nbots, cfg);
		}
		nbots.putAll(this.systemBots);
		for(String id : nbots.keySet()) {
			initBotPreference(id);
		}
		Collection<PublicAPIEndpoint<?, ?>> endpoints = publicDataManager.getEndpoints().values();
		for(PublicAPIEndpoint<?, ?> papi : endpoints) {
			PublicDataUpdateBot<?, ?> bt = new PublicDataUpdateBot<>(papi);
			if(!nbots.containsKey(bt.getId())) {
				nbots.put(bt.getId(), bt);
			}
		}
		this.bots = nbots;
		blc.setCacheAfterSearch(req, nbots);
		return nbots;
	}

	private void recreateBotInstance(Map<String, IOpenDBBot<?>> nbots, OpObject cfg) {
		String id = cfg.getId().get(0);
		String api = cfg.getStringValue("api");
		IOpenDBBot<?> exBot = nbots.get(id);
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

	private CommonPreference<Map<String, Object>> initBotPreference(String id) {
		CommonPreference<Map<String, Object>> p = settings.getPreferenceByKey(OPENDB_BOTS_CONFIG.getId(id));
		if(p == null) {
			TreeMap<String, Object> mp = new TreeMap<>();
			mp.put(SettingsManager.BOT_ID, id);
			mp.put(BOT_ENABLED, false);
			p = settings.registerMapPreferenceForFamily(SettingsManager.OPENDB_BOTS_CONFIG, mp);
		}
		return p;
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
		if (p == null) {
			p = settings.getPreferenceByKey(OPENDB_ENDPOINTS_CONFIG.getId(botId));
		}
		return (MapStringObjectPreference) p;
	}
	
	
	public boolean enableBot(String botId, int intervalSeconds) {
		MapStringObjectPreference p = getBotConfiguration(botId);
		if (p == null) {
			return false;
		}
		p.setValue(BOT_INTERVAL_SECONDS, intervalSeconds, false)
		 .setValue(BOT_ENABLED, true, true);
		return true;
	}
	
	public boolean disableBot(String botId) {
		MapStringObjectPreference p = getBotConfiguration(botId);
		if (p == null) {
			return false;
		}
		p.setValue(BOT_ENABLED, false, true);
		return true;
	}


	public void runBotsBySchedule() {
		Map<String, IOpenDBBot<?>> bs = this.bots;
		long now = System.currentTimeMillis() / 1000;
		for(String bid : bs.keySet()) {
			MapStringObjectPreference p = getBotConfiguration(bid);
			if(p.getBoolean(BOT_ENABLED, false)) {
				long lastRun = p.getLong(SettingsManager.BOT_LAST_RUN, 0);
				long l = p.getLong(BOT_INTERVAL_SECONDS, 0);
				if(now - lastRun > l) {
					startBot(bid);
				}
			}
		}
	}
	
	
}
