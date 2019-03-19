package org.openplacereviews.opendb.service;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.ValidationTimer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class BlocksManager {
	protected static final Log LOGGER = LogFactory.getLog(BlocksManager.class);
	
	@Autowired
	private LogOperationService logSystem;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	private DBConsensusManager dataManager;
	
	@Value("${opendb.user}")
	private String serverUser;
	
	@Value("${opendb.privateKey}")
	private String serverPrivateKey;
	
	@Value("${opendb.publicKey}")
	private String serverPublicKey;
	private KeyPair serverKeyPair;
	
	private OpBlockChain blockchain; 
	
	public String getServerPrivateKey() {
		return serverPrivateKey;
	}
	
	public String getServerUser() {
		return serverUser;
	}
	
	public KeyPair getServerLoginKeyPair() {
		return serverKeyPair;
	}
	
	public synchronized boolean addOperation(OpOperation op) {
		if(blockchain == null) {
			return false;
		}
		boolean added = blockchain.addOperation(op);
		dataManager.insertOperation(op);
		return added;
	}
	
	public synchronized void clearQueue() {
		// there is no proper clear queue on atomc load
		blockchain = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
	}
	
	public synchronized boolean revertSuperblock() throws FailedVerificationException {
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		if(blockchain.getParent() == null) {
			return false;
		}
		OpBlockChain blc = new OpBlockChain(blockchain.getParent().getParent(), blockchain.getRules());
		OpBlockChain pnt = blockchain.getParent();
		List<OpBlock> lst = new ArrayList<OpBlock>(pnt.getOneSuperBlock());
		Collections.reverse(lst);
		for(OpBlock bl :  lst) {
			for (OpOperation u : bl.getOperations()) {
				if (!blc.addOperation(u)) {
					return false;
				}
			}
		}
		for(OpOperation o: blockchain.getOperations()) {
			if(!blc.addOperation(o)) {
				return false;
			}
		}
		OpBlockChain p = blockchain;
		blockchain = blc;
		String msg = String.format("Revert superblock from '%s:%d' to '%s:%d'", 
				p.getLastHash(), p.getLastBlockId(), blockchain.getLastHash(), blockchain.getLastBlockId());
		logSystem.logSuccessBlock(blockchain.getLastBlock(), msg);
		return true;
	}
	
	public synchronized OpBlock createBlock() throws FailedVerificationException {
		// should be changed synchronized in future:
		// This method doesn't need to be full synchronized cause it could block during compacting or any other operation adding ops
		
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		ValidationTimer timer = new ValidationTimer();
		timer.start();
		
		List<OpOperation> candidates = pickupOpsFromQueue(blockchain.getOperations());
		
		int tmAddOps = timer.startExtra();
		OpBlockChain blc = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
		for (OpOperation o : candidates) {
			if(!blc.addOperation(o)) {
				return null;
			}
		}
		timer.measure(tmAddOps, ValidationTimer.BLC_ADD_OPERATIONS);
		
		int tmNewBlock = timer.startExtra();
		OpBlock opBlock = blc.createBlock(serverUser, serverKeyPair);
		if(opBlock == null) {
			return null;
		}
		timer.measure(tmNewBlock, ValidationTimer.BLC_NEW_BLOCK);
		
		int tmDbSave = timer.startExtra();
		dataManager.insertBlock(opBlock);
		timer.measure(tmDbSave, ValidationTimer.BLC_BLOCK_SAVE);
		
		// change only after block is inserted into db
		int tmRebase = timer.startExtra();
		boolean changeParent = blockchain.changeParent(blc);
		if(!changeParent) {
			return null;
		}
		timer.measure(tmRebase, ValidationTimer.BLC_REBASE);
		
		int tmSDbSave = timer.startExtra();
		dataManager.saveMainBlockchain(blockchain.getParent());
		timer.measure(tmSDbSave, ValidationTimer.BLC_SAVE);
		
		
		int tmCompact = timer.startExtra();
		dataManager.compact(blockchain.getParent());
		timer.measure(tmCompact, ValidationTimer.BLC_COMPACT);
		
		
		opBlock.putCacheObject(OpObject.F_VALIDATION, timer.getTimes());
		logSystem.logSuccessBlock(opBlock, 
				String.format("New block '%s':%d  is created on top of '%s'. ",
						opBlock.getHash(), opBlock.getBlockId(), opBlock.getStringValue(OpBlock.F_PREV_BLOCK_HASH) ));
		return opBlock;
	}
	
	public OpBlockChain getBlockchain() {
		return blockchain == null ? OpBlockChain.NULL : blockchain;
	}

	public synchronized void init(MetadataDb metadataDB, OpBlockChain initBlockchain) {
		LOGGER.info("... Blockchain. Loading blocks...");
		try {
			this.serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverPrivateKey, serverPublicKey);
		} catch (FailedVerificationException e) {
			LOGGER.error("Error validating server private / public key: " + e.getMessage(), e);
			throw new RuntimeException(e);
		}
		this.blockchain = initBlockchain;
		
		String msg = "";
		// db is bootstraped
		LOGGER.info("+++ Blockchain is inititialized. " + msg);
	}
	
	
	public synchronized boolean resumeBlockCreation() {
		if(blockchain.getStatus() == OpBlockChain.LOCKED_SUCCESS) {
			blockchain.makeMutable();
			return true;
		}
		return false;
	}
	
	public synchronized boolean pauseBlockCreation() {
		if(blockchain.getStatus() == OpBlockChain.UNLOCKED) {
			blockchain.makeImmutable();
			return true;
		}
		return false;
	}
	
	public String getCurrentState() {
		if(blockchain.getStatus() == OpBlockChain.UNLOCKED) {
			return "READY";
		} else if(blockchain.getStatus() == OpBlockChain.LOCKED_SUCCESS) {
			return "LOCKED";
		}
		return "ERROR";
	}
	
	public boolean isBlockchainPaused() {
		return blockchain.getStatus() != OpBlockChain.UNLOCKED;
	}
	
	public OpBlock getLastBlock() {
		return blockchain.getLastBlock();
	}

	
	public OpOperation generateHashAndSign(OpOperation op, KeyPair... keyPair) throws FailedVerificationException {
		return blockchain.getRules().generateHashAndSign(op, keyPair);
	}
	
	public KeyPair getLoginKeyPairFromPwd(String name, String pwd) throws FailedVerificationException {
		return blockchain.getRules().getSignUpKeyPairFromPwd(blockchain, name, pwd);
	}
	
	public KeyPair getLoginKeyPair(String name, String privateKey) throws FailedVerificationException {
		return blockchain.getRules().getLoginKeyPair(blockchain, name, privateKey);
	}

	public OpObject getLoginObj(String nickname) {
		return blockchain.getRules().getLoginKeyObj(blockchain, nickname);
	}

	private List<OpOperation> pickupOpsFromQueue(Collection<OpOperation> q) {
		int size = 0;
		List<OpOperation> candidates = new ArrayList<OpOperation>();
		for (OpOperation o : q) {
			int l = formatter.opToJson(o).length();
			if (size + l > OpBlockchainRules.MAX_BLOCK_SIZE_MB) {
				break;
			}
			if (candidates.size() + 1 >= OpBlockchainRules.MAX_BLOCK_SIZE_OPS) {
				break;
			}
			candidates.add(o);
		}
		return candidates;
	}	

	
}
