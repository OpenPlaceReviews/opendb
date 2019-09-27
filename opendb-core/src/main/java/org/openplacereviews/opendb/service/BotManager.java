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
import java.util.concurrent.TimeUnit;

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
		long started;
		IOpenDBBot<?> instance;
		String interval;
		BotStats botStats;

		public long getStarted() {
			return started;
		}
		
		public String getId() {
			return id;
		}
		
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
			Map<String, BotInfo> botInfoMap = (Map<String, BotInfo>) req.cacheObject;
			// TODO right place to generate bot stats?
			for (BotInfo botInfo : botInfoMap.values()) {
				botInfo.botStats = generateBotStats(botInfo);
			}
			return botInfoMap;
		}
		return recreateBots(req, blc);
	}

	public Map<String, BotInfo> getBotInfo() {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		req.requestCache = true;
		OpBlockChain blc = blocksManager.getBlockchain();
		blc.fetchAllObjects(OpBlockchainRules.OP_BOT, req);
		return recreateBots(req, blc);
	}

	private synchronized Map<String, BotManager.BotInfo> recreateBots(OpBlockChain.ObjectsSearchRequest req, OpBlockChain blc) {
		TreeSet<String> inits = new TreeSet<>(this.bots.keySet());
		Map<String, BotManager.BotInfo> nbots = new TreeMap<String, BotManager.BotInfo>();
		for (OpObject cfg : req.result) {
			BotInfo bi = new BotInfo();
			bi.id = cfg.getId().get(0);
			bi.api = cfg.getStringValue("api");
			// TODO 
			long timeInMilis = 30000;
			bi.interval = String.format("%02dh:%02dm:%02ds", TimeUnit.MILLISECONDS.toHours(timeInMilis),
					TimeUnit.MILLISECONDS.toMinutes(timeInMilis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeInMilis)),
					TimeUnit.MILLISECONDS.toSeconds(timeInMilis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeInMilis)));
			inits.remove(bi.id);
			BotInfo exBot = this.bots.get(bi.id);
			if (exBot == null || !exBot.api.equals(bi.api)) {
				try {

					Class<?> bot = Class.forName(bi.api);
					Constructor<?> constructor = bot.getConstructor(OpObject.class);
					bi.instance = (IOpenDBBot<?>) constructor.newInstance(cfg);
					beanFactory.autowireBean(bi.getInstance());
				} catch (Exception e) {
					LOGGER.error(String.format("Error while creating bot %s instance api %s", bi.id, bi.api), e);
				}
			} else {
				bi = exBot;
			}
			if (bi.getInstance() != null) {
				bi.botStats = generateBotStats(bi);
			}
			nbots.put(bi.id, bi);
		}
		this.bots = nbots;
		blc.setCacheAfterSearch(req, nbots);
		return nbots;
	}

	private BotStats generateBotStats(BotInfo bi) {
		if (bi.getInstance() != null) {
			return new BotStats(
					bi.getInstance().getTaskDescription(),
					bi.getInstance().getTaskName(),
					bi.getInstance().taskCount(),
					bi.getInstance().total(),
					bi.getInstance().progress(),
					bi.getInstance().isRunning()
			);
		};

		return null;
	}
	
	public boolean startBot(String botId) {
		// TODO separate enable / start
		BotInfo botObj = getBots().get(botId);
		if (botObj == null) {
			return false;
		}
		if (botObj.instance != null) {
			botObj.started = System.currentTimeMillis();
			futures.add(service.submit(botObj.instance));
			return true;
		}

		return false;
	}
	
	public boolean stopBot(String botId) {
		// TODO separate enable / stop
		BotInfo botObj = getBots().get(botId);
		if (botObj == null) {
			return false;
		}
		if (botObj.instance != null) {
			return botObj.instance.interrupt();
		}
		return false;
	}

	public List<BotHistory> getBotHistory(String botName) {
		// TODO
		return new ArrayList<BotManager.BotHistory>();
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

		public BotStats(String taskDescription, String taskName, int taskCount, int total, int progress, boolean isRunning) {
			this.taskDescription = taskDescription;
			this.taskName = taskName;
			this.taskCount = taskCount;
			this.total = total;
			this.progress = progress;
			this.isRunning = isRunning;
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
