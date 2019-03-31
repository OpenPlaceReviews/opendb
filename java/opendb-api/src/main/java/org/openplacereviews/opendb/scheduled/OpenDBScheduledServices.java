package org.openplacereviews.opendb.scheduled;

import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.service.BlocksManager;
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
	
	
	private static final int BLOCK_CREATION_PULSE_INTERVAL_SECONDS = 15;
	
	@Value("${opendb.block-create.minSecondsInterval}")
	public int minSecondsInterval = BLOCK_CREATION_PULSE_INTERVAL_SECONDS;
	
	@Value("${opendb.block-create.minQueueSize}")
	public int minQueueSize = 10;
	
	@Value("${opendb.block-create.maxSecondsInterval}")
	public int maxSecondsInterval = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * 20;
	
	@Value("${opendb.replicate.interval}")
	public int replicateInterval = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * 10;

	private long previousReplicateCheck = 0;
	
	@Autowired
	private BlocksManager blocksManager;
	
	@Scheduled(fixedRate = BLOCK_CREATION_PULSE_INTERVAL_SECONDS * SECOND)
	public void replicateBlock() throws FailedVerificationException {
		if(blocksManager.isReplicateOn()) {
			long now = System.currentTimeMillis() / 1000;
			if(previousReplicateCheck - now > replicateInterval) {
				blocksManager.replicate();
				// ignore if replication was successful or not
				// exception would mean network failure and conflicts will need to be resolved manually
				previousReplicateCheck = now;
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
			boolean createBlock = false; 
			if(timePast > maxSecondsInterval) {
				createBlock = true;
			} else if(timePast > minSecondsInterval && sz >= minQueueSize ) {
				createBlock = true;
			}
			if(createBlock) {
				blocksManager.createBlock();
			}
		}
	}
}

