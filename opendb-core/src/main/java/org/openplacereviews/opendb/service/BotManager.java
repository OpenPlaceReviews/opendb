package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
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

	private Map<String, BotInfo> bots = new TreeMap<String, BotManager.BotInfo>();

	List<Future<?>> futures = new ArrayList<>();
	ExecutorService service = Executors.newFixedThreadPool(5);
	
	public static class BotInfo {
		String api;
		String id;
		int version;
		IOpenDBBot<?> instance;
		
		public IOpenDBBot<?> getInstance() {
			return instance;
		}

	}

	@SuppressWarnings("unchecked")
	public Map<String, BotInfo> getBots() {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		req.requestCache = true;
		OpBlockChain blc = blocksManager.getBlockchain();
		blc.fetchAllObjects(OpBlockchainRules.OP_BOT, req);
		if (req.cacheObject != null) {
			return (Map<String, BotInfo>) req.cacheObject;
		}
		return recreateBots(req, blc);
	}


	private synchronized Map<String, BotManager.BotInfo> recreateBots(OpBlockChain.ObjectsSearchRequest req, OpBlockChain blc) {
		TreeSet<String> inits = new TreeSet<>(this.bots.keySet());
		Map<String, BotManager.BotInfo> nbots = new TreeMap<String, BotManager.BotInfo>();
		for (OpObject cfg : req.result) {
			BotInfo bi = new BotInfo();
			bi.id = cfg.getId().get(0);
			bi.version = cfg.getIntValue("version", 0);
			bi.api = cfg.getStringValue("api");
			inits.remove(bi.id);
			BotInfo exBot = this.bots.get(bi.id);
			if (exBot == null || !exBot.api.equals(bi.api)) {
				try {

					Class<?> bot = Class.forName(bi.api);
					Constructor<?> constructor = bot.getConstructor(OpObject.class);
					bi.instance = (IOpenDBBot<?>) constructor.newInstance(cfg);
					beanFactory.autowireBean(bi.getInstance());
				} catch (Exception e) {
					LOGGER.error(String.format("Error while creating bot %s instance version %d, api %s", bi.id,
							bi.version, bi.api), e);
				}
			} else {
				bi = exBot;
			}
			nbots.put(bi.id, bi);
		}
		this.bots = nbots;
		blc.setCacheAfterSearch(req, nbots);
		return nbots;
	}
	
	
	public boolean startBot(String botId) {
		BotInfo botObj = getBots().get(botId);
		if (botObj == null) {
			return false;
		}
		if (botObj.instance != null) {
			futures.add(service.submit(botObj.instance));
			return true;
		}

		return false;
	}
	
	public boolean stopBot(String botId) {
		BotInfo botObj = getBots().get(botId);
		if (botObj == null) {
			return false;
		}
		if (botObj.instance != null) {
			return botObj.instance.interrupt();
		}
		return false;
	}

}
