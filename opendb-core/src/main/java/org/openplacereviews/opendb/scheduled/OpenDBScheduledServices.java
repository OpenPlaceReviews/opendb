package org.openplacereviews.opendb.scheduled;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.service.BlocksManager;
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
	
	@Value("${opendb.block-create.minSecondsInterval}")
	public int minSecondsInterval = BLOCK_CREATION_PULSE_INTERVAL_SECONDS;
	
	@Value("${opendb.block-create.minCapacity}")
	public double minCapacity = 0.7;
	
	@Value("${opendb.block-create.maxSecondsInterval}")
	public int maxSecondsInterval = 60 * 15;
	
	@Value("${opendb.replicate.interval}")
	public int replicateInterval = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * 10;

	private long previousReplicateCheck = 0;
	
	@Autowired
	private BlocksManager blocksManager;
	
	@Scheduled(fixedRate = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * SECOND)
	public void replicateBlock() {
		if(blocksManager.isReplicateOn()) {
			try {
				long now = System.currentTimeMillis() / 1000;
				if (now - previousReplicateCheck > replicateInterval) {
					blocksManager.replicate();
					// ignore if replication was successful or not
					// exception would mean network failure and conflicts will need to be resolved manually
					previousReplicateCheck = now;
					LOGGER.info(String.format("Replication successful %s", new Date())); 
							//now, previousReplicateCheck, replicateInterval));
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
			OpBlock hd = blocksManager.getBlockchain().getLastBlockHeader();
			long timePast = (System.currentTimeMillis() - (hd == null ?  0 : hd.getDate(OpBlock.F_DATE))) / 1000; 
			double cp = blocksManager.getQueueCapacity();
			if(timePast > maxSecondsInterval) {
				blocksManager.createBlock(0);
			} else if(timePast > minSecondsInterval && cp >= minCapacity) {
				blocksManager.createBlock(minCapacity);
			}
		}
	}
}

