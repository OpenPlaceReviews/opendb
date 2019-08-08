package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_BOT;

@Service
public class BotManager {

	private static final Log LOGGER = LogFactory.getLog(BotManager.class);

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	List<Future<?>> futures = new ArrayList<>();
	ExecutorService service = Executors.newFixedThreadPool(5);

	public Set<List<String>> getAllBots() {
		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blocksManager.getBlockchain().getObjectHeaders(OP_BOT, objectsSearchRequest);
		return objectsSearchRequest.resultWithHeaders;
	}

	public boolean startBot(String botName) {
		OpObject botObject = blocksManager.getBlockchain().getObjectByName(OP_BOT, botName);
		if (botObject == null) {
			return false;
		}

		IPublishBot botInterface = null;
		try {
			Class<?> bot = Class.forName(botObject.getStringObjMap("bot").get("API").toString());
			Constructor constructor = bot.getConstructor(BlocksManager.class, OpObject.class, JdbcTemplate.class);
			botInterface = (IPublishBot) constructor.newInstance(blocksManager, botObject, jdbcTemplate);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
			LOGGER.error("Error while creating bot instance", e);
		}

		if (botInterface != null) {
			futures.add(service.submit(botInterface));
			return true;
		}

		return false;
	}

	// TODO add some info
	public Map<List<String>, String> getBotStats() {
		return Collections.EMPTY_MAP;
	}
}
