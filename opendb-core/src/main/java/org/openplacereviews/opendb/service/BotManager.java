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
		// TODO separate enable / start
		IOpenDBBot<?> botObj = getBots().get(botId);
		if (botObj == null) {
			return false;
		}
		futures.add(service.submit(botObj));
		return true;
	}
	
	public boolean stopBot(String botId) {
		// TODO separate enable / stop
		IOpenDBBot<?>  botObj = getBots().get(botId);
		if (botObj == null) {
			return false;
		}
		return botObj.interrupt();
	}

	public List<BotHistory> getBotHistory(String botName) {
		// TODO
		return new ArrayList<BotManager.BotHistory>();
	}

	public void runBotsBySchedule() {
		// TODO Auto-generated method stub
		
	}
	
	public static class BotHistory {
		public String bot, status;
		public Date startDate, endDate;
		public Integer total, processed;
	}
	
	public static class BotStats {
		String taskDescription;
		String taskName;
		int taskCount;
		int total;
		int progress;
		boolean isRunning;

		public BotStats(IOpenDBBot<?> i) {
			this.taskDescription = i.getTaskDescription();
			this.taskName = i.getTaskName();
			this.taskCount = i.taskCount();
			this.total = i.total();
			this.progress = i.progress();
			this.isRunning = i.isRunning();
		}

		public String getTaskDescription() {
			return taskDescription;
		}

		public String getTaskName() {
			return taskName;
		}

		public int getTaskCount() {
			return taskCount;
		}

		public int getTotal() {
			return total;
		}

		public int getProgress() {
			return progress;
		}

		public boolean isRunning() {
			return isRunning;
		}
	}

	
}
