package org.openplacereviews.opendb.service;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;


@Service
public class BlocksManager {
	protected static final Log LOGGER = LogFactory.getLog(BlocksManager.class);
	
	@Autowired
	private LogOperationService logSystem;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	JsonFormatter formatter = new JsonFormatter();
	
	@Value("${opendb.user}")
	private String serverUser;
	
	@Value("${opendb.privateKey}")
	private String serverPrivateKey;
	
	@Value("${opendb.publicKey}")
	private String serverPublicKey;
	
	
	private OpBlockChain blockchain; 
	private OpBlockchainRules blockchainRules;
	
	public OpOperation generateHashAndSign(OpOperation op, KeyPair... keyPair) throws FailedVerificationException {
		return blockchainRules.generateHashAndSign(op, keyPair);
	}

	public String getServerPrivateKey() {
		return serverPrivateKey;
	}
	
	public String getServerUser() {
		return serverUser;
	}
	
	public KeyPair getServerLoginKeyPair() {
		return blockchainRules.getServerKeyPair();
	}
	
	public synchronized boolean addOperation(OpOperation op) {
		// TODO LOG success or failure (create queue of failed) ops
		return blockchain.addOperation(op, blockchainRules);
	}
	
	public synchronized void clearQueue() {
		// there is no proper clear queue on atomc load
		blockchain = new OpBlockChain(blockchain.getParent());
	}
	
	public synchronized boolean revertSuperblock() throws FailedVerificationException {
		// TODO add logging for failures like addOperation, createBlock, changeParent
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		if(blockchain.getParent() == null) {
			return false;
		}
		OpBlockChain blc = new OpBlockChain(blockchain.getParent().getParent());
		OpBlockChain pnt = blockchain.getParent();
		for(OpBlock bl :  pnt.getSubchainBlocks()) {
			for (OpOperation u : bl.getOperations()) {
				if (!blc.addOperation(u, blockchainRules)) {
					return false;
				}
			}
		}
		for(OpOperation o: blockchain.getOperations()) {
			if(!blc.addOperation(o, blockchainRules)) {
				return false;
			}
		}
		blockchain = blc;
		return true;
	}
	
	public synchronized OpBlock createBlock() throws FailedVerificationException {
		// should be changed synchronized in future:
		// This method doesn't need to be full synchronized cause it could block during compacting or any other operation adding ops
		
		// TODO add logging for failures like addOperation, createBlock, changeParent
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		ValidationTimer timer = new ValidationTimer();
		timer.start();
		
		List<OpOperation> candidates = pickupOpsFromQueue(blockchain.getOperations());
		
		int tmAddOps = timer.startExtra();
		OpBlockChain blc = new OpBlockChain(blockchain.getParent());
		for (OpOperation o : candidates) {
			if(!blc.addOperation(o, blockchainRules)) {
				return null;
			}
		}
		timer.measure(tmAddOps, ValidationTimer.BLC_ADD_OPERATIONS);
		
		int tmNewBlock = timer.startExtra();
		OpBlock opBlock = blc.createBlock(blockchainRules, timer);
		if(opBlock == null) {
			return null;
		}
		timer.measure(tmNewBlock, ValidationTimer.BLC_NEW_BLOCK);
		
		
		int tmRebase = timer.startExtra();
		boolean changeParent = blockchain.changeParent(blc);
		if(!changeParent) {
			return null;
		}
		timer.measure(tmRebase, ValidationTimer.BLC_REBASE);
		
		
		int tmCompact = timer.startExtra();
		blc.compact();
		timer.measure(tmCompact, ValidationTimer.BLC_COMPACT);
		
		timer.measure(ValidationTimer.BLC_TOTAL_BLOCK);
		opBlock.putObjectValue(OpObject.F_VALIDATION, timer.getTimes());
		return opBlock;
	}
	
	public OpBlockChain getBlockchain() {
		return blockchain;
	}

	public synchronized void init(MetadataDb metadataDB) {
		LOGGER.info("... Blockchain. Loading blocks...");
		KeyPair serverKeyPair;
		try {
			serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverPrivateKey, serverPublicKey);
		} catch (FailedVerificationException e) {
			LOGGER.error("Error validating server private / public key: " + e.getMessage(), e);
			throw new RuntimeException(e);
		}
		blockchainRules = new OpBlockchainRules(formatter, serverUser, serverKeyPair);
		blockchain = new OpBlockChain(null);
		
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

	public KeyPair getLoginKeyPairFromPwd(String name, String pwd) throws FailedVerificationException {
		return blockchainRules.getSignUpKeyPairFromPwd(blockchain, name, pwd);
	}
	
	public KeyPair getLoginKeyPair(String name, String privateKey) throws FailedVerificationException {
		return blockchainRules.getLoginKeyPair(blockchain, name, privateKey);
	}

	public OpObject getLoginObj(String nickname) {
		return blockchainRules.getLoginKeyObj(blockchain, nickname);
	}

	private List<OpOperation> pickupOpsFromQueue(Collection<OpOperation> q) {
		int size = 0;
		List<OpOperation> candidates = new ArrayList<OpOperation>();
		for (OpOperation o : q) {
			int l = formatter.toJson(o).length();
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
