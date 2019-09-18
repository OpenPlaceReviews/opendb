package org.openplacereviews.opendb.scheduled;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.BotManager;
import org.openplacereviews.opendb.service.SettingsManager;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class OpenDBScheduledServices {
	public static final int SECOND = 1000;
	public static final int MINUTE = 60 * SECOND;
	public static final int HOUR = 60 * MINUTE;
	public static final long DAY = 24l * HOUR;
	
	protected static final Log LOGGER = LogFactory.getLog(OpenDBScheduledServices.class);
	protected static final int BLOCK_CREATION_PULSE_INTERVAL_SECONDS = 15;

	private long previousReplicateCheck = 0;
	private long previousBotsCheck = 0;
	private long opsAppeared = 0;
	
	@Autowired
	private BlocksManager blocksManager;


	@Autowired
	private BotManager botManager;
	
	@Autowired
	private SettingsManager settingsManager;
	
	@Scheduled(fixedRate = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * SECOND)
	public void replicateBlock() {
		if(blocksManager.isReplicateOn()) {
			try {
				long now = System.currentTimeMillis() / 1000;
				if (now - previousReplicateCheck > getReplicateInterval()) {
					int d = blocksManager.getBlockchain().getDepth();
					blocksManager.replicate();
					// ignore if replication was successful or not
					// exception would mean network failure and conflicts will need to be resolved manually
					previousReplicateCheck = now;
					if(blocksManager.getBlockchain().getDepth() != d) {
						LOGGER.info(String.format("Replication successful %s", new Date())); 
							//now, previousReplicateCheck, replicateInterval));
					}
				}
			} catch (Exception e) {
				LOGGER.error("Error replication: " + e.getMessage(), e);
			}
		}
	}
	
	@Scheduled(fixedRate = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * SECOND, initialDelay = MINUTE)
	public void runBots() {
		long now = System.currentTimeMillis();
		if (blocksManager.getBlockchain().getStatus() == OpBlockChain.UNLOCKED && blocksManager.isBlockCreationOn()
				&& (now - previousBotsCheck) >= getBotsMinInterval()) {
			previousBotsCheck = now;
			for (BotManager.BotInfo bi : botManager.getBots().values()) {
				botManager.startBot(bi.getId());
			}
		}
	}
	
	@Scheduled(fixedRate = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * SECOND)
	public void createBlock() throws FailedVerificationException {
		int sz = blocksManager.getBlockchain().getQueueOperations().size();
		if(blocksManager.getBlockchain().getStatus() == OpBlockChain.UNLOCKED && sz > 0 && 
				blocksManager.isBlockCreationOn()) {
//			OpBlock hd = blocksManager.getBlockchain().getLastBlockHeader();
//			long timePast = (System.currentTimeMillis() - (hd == null ?  0 : hd.getDate(OpBlock.F_DATE))) / 1000;
			if (opsAppeared == 0) {
				opsAppeared = System.currentTimeMillis();
			}
			long timePast = (System.currentTimeMillis() - opsAppeared) / 1000;
			double cp = blocksManager.getQueueCapacity();
			if(timePast > getMaxSecondsInterval()) {
				blocksManager.createBlock(0);
				opsAppeared = 0;
			} else if(timePast > getMinSecondsInterval() && cp >= getMinCapacity()) {
				blocksManager.createBlock(getMinCapacity());
			}
		} else if(sz == 0) {
			opsAppeared = 0;
		}
	}

	public int getReplicateInterval() {
		return settingsManager.OPENDB_REPLICATE_INTERVAL.get();
	}

	public int getMaxSecondsInterval() {
		return settingsManager.OPENDB_BLOCK_CREATE_MAX_SECONDS_INTERVAL.get();
	}

	public double getMinCapacity() {
		return settingsManager.OPENDB_BLOCK_CREATE_MIN_CAPACITY.get();
	}

	public int getMinSecondsInterval() {
		return settingsManager.OPENDB_BLOCK_CREATE_MIS_SECONDS_INTERVAL.get();
	}

	public Long getBotsMinInterval() {
		return settingsManager.OPENDB_BOTS_MIN_INTERVAL.get() * 1000L;
	}

}

