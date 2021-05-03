package org.openplacereviews.opendb.service;

import static org.openplacereviews.opendb.service.SettingsManager.BOT_ENABLED;
import static org.openplacereviews.opendb.service.SettingsManager.BOT_INTERVAL_SECONDS;
import static org.openplacereviews.opendb.service.SettingsManager.OPENDB_BOTS_CONFIG;
import static org.openplacereviews.opendb.service.SettingsManager.OPENDB_ENDPOINTS_CONFIG;
import static org.openplacereviews.opendb.service.bots.PublicDataUpdateBot.PUBLIC_DATA_BOT_NAME_PREFIX;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.openplacereviews.opendb.service.SettingsManager.CommonPreference;
import org.openplacereviews.opendb.service.SettingsManager.MapStringObjectPreference;
import org.openplacereviews.opendb.service.bots.IOpenDBBot;
import org.openplacereviews.opendb.service.bots.PublicDataUpdateBot;
import org.openplacereviews.opendb.service.bots.UpdateIndexesBot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Service;

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
	private int publicDataManagerVersion;

	private Map<String, IOpenDBBot<?>> bots = new TreeMap<String, IOpenDBBot<?>>();
	private Map<String, IOpenDBBot<?>> systemBots = new TreeMap<String, IOpenDBBot<?>>();
	private ScheduledExecutorService service = Executors.newScheduledThreadPool(5);
	private Map<String, ScheduledFuture<?>> scheduledFutures = new ConcurrentHashMap<String, ScheduledFuture<?>>();
	

	@PostConstruct
	public void initSystemBots() {
		regSystemBot(new UpdateIndexesBot("update-indexes"));
	}

	public void regSystemBot(IOpenDBBot<?> bt) {
		beanFactory.autowireBean(bt);
		systemBots.put(bt.getId(), bt);
	}

	@SuppressWarnings("unchecked")
	public Map<String, IOpenDBBot<?>> getBots() {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		req.requestCache = true;
		OpBlockChain blc = blocksManager.getBlockchain();
		blc.fetchAllObjects(OpBlockchainRules.OP_BOT, req);
		int publicDataManagerVersion = publicDataManager.getVersion();
		if (req.cacheObject != null && publicDataManagerVersion == this.publicDataManagerVersion) {
			return (Map<String, IOpenDBBot<?>>) req.cacheObject;
		}
		this.publicDataManagerVersion = publicDataManagerVersion;
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
		for (PublicAPIEndpoint<?, ?> papi : endpoints) {
			PublicDataUpdateBot<?, ?> bt = new PublicDataUpdateBot<>(papi);
			beanFactory.autowireBean(bt);
			if (!nbots.containsKey(bt.getId())) {
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



	@SuppressWarnings("unchecked")
	public <T> boolean startBot(String botId) {
		IOpenDBBot<T> botObj = (IOpenDBBot<T>) getBots().get(botId);
		MapStringObjectPreference p = getBotConfiguration(botId);
		if (botObj == null || p == null) {
			return false;
		}
		ScheduledFuture<?> scheduledFuture = scheduledFutures.get(botId);
		if (scheduledFuture != null && scheduledFuture.getDelay(TimeUnit.MILLISECONDS) > 0 &&
				!scheduledFuture.isDone()) {
			// bot is already scheduled
			// TODO: to test
			return true;
		}
		scheduledFuture = service.schedule(new Callable<T>() {

			public T call() throws Exception {
				p.setValue(SettingsManager.BOT_LAST_RUN, System.currentTimeMillis() / 1000, true);
				return botObj.call();
			}
		}, 50, TimeUnit.MILLISECONDS);
		scheduledFutures.put(botId, scheduledFuture);
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
		CommonPreference<Map<String, Object>> p;
		if (botId.startsWith(PUBLIC_DATA_BOT_NAME_PREFIX)) {
			p = settings.getPreferenceByKey(OPENDB_ENDPOINTS_CONFIG.getId(botId.substring(PUBLIC_DATA_BOT_NAME_PREFIX.length())));
		} else {
			p = settings.getPreferenceByKey(SettingsManager.OPENDB_BOTS_CONFIG.getId(botId));
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
		for (String bid : bs.keySet()) {
			MapStringObjectPreference p = getBotConfiguration(bid);
			if (p.getBoolean(BOT_ENABLED, false)) {
				long lastRun = p.getLong(SettingsManager.BOT_LAST_RUN, 0);
				long l = p.getLong(BOT_INTERVAL_SECONDS, 0);
				if (now - lastRun > l) {
					startBot(bid);
				}
			}
		}
	}
	
	
}
