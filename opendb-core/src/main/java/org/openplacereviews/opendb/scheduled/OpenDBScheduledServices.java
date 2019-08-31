package org.openplacereviews.opendb.scheduled;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.BotManager;
import org.openplacereviews.opendb.service.BotManager.BotInfo;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OpenDBScheduledServices {
	protected static final int SECOND = 1000;
	protected static final int MINUTE = 60 * SECOND;
	protected static final int HOUR = 60 * MINUTE;
	protected static final long DAY = 24l * HOUR;
	
	protected static final Log LOGGER = LogFactory.getLog(OpenDBScheduledServices.class);
	private static final int BLOCK_CREATION_PULSE_INTERVAL_SECONDS = 15;
	
	@Value("${opendb.block-create.minSecondsInterval:15}")
	public int minSecondsInterval = BLOCK_CREATION_PULSE_INTERVAL_SECONDS;
	
	@Value("${opendb.block-create.minCapacity:0.8}")
	public double minCapacity = 0.7;
	
	@Value("${opendb.block-create.maxSecondsInterval:900}")
	public int maxSecondsInterval = 60 * 15;
	
	@Value("${opendb.replicate.interval:30}")
	public int replicateInterval = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * 2;
	
	@Value("${opendb.bots.minInterval:1800}")
	public int botsMinInterval = 1800;

	private long previousReplicateCheck = 0;
	
	private long previousBotsCheck = 0;
	
	private long opsAppeared = 0;
	
	@Autowired
	private BlocksManager blocksManager;
	
	@Autowired
	private BotManager botManager;
	
	@Scheduled(fixedRate = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * SECOND)
	public void runBots() throws FailedVerificationException {
		long now = System.currentTimeMillis();
		if (blocksManager.getBlockchain().getStatus() == OpBlockChain.UNLOCKED && blocksManager.isBlockCreationOn()
				&& (now - previousBotsCheck) >= botsMinInterval * 1000l) {
			previousBotsCheck = now;
			for (BotInfo bi : botManager.getBots().values()) {
				botManager.startBot(bi.getId());
			}
		}
	}
	
	@Scheduled(fixedRate = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * SECOND)
	public void replicateBlock() {
		if(blocksManager.isReplicateOn()) {
			try {
				long now = System.currentTimeMillis() / 1000;
				if (now - previousReplicateCheck > replicateInterval) {
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
			if(timePast > maxSecondsInterval) {
				blocksManager.createBlock(0);
				opsAppeared = 0;
			} else if(timePast > minSecondsInterval && cp >= minCapacity) {
				blocksManager.createBlock(minCapacity);
			}
		} else if(sz == 0) {
			opsAppeared = 0;
		}
	}
}

