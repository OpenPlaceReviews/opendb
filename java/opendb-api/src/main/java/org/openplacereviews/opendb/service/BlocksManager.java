package org.openplacereviews.opendb.service;

import java.io.InputStreamReader;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.MgmtController;
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
	
	public String[] BOOTSTRAP_LIST = new String[] {"opr-0-test", "std-ops-defintions", "std-roles", "std-validations"};
	
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
		op.makeImmutable();
		boolean added = blockchain.addOperation(op);
		dataManager.insertOperation(op);
		return added;
	}
	
	public synchronized OpBlock createBlock() throws FailedVerificationException {
		// should be changed synchronized in future:
		// This method doesn't need to be full synchronized cause it could block during compacting or any other operation adding ops
		
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		ValidationTimer timer = new ValidationTimer();
		timer.start();
		
		List<OpOperation> candidates = pickupOpsFromQueue(blockchain.getQueueOperations());
		
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
		boolean changeParent = blockchain.rebaseOperations(blc);
		if(!changeParent) {
			return null;
		}
		timer.measure(tmRebase, ValidationTimer.BLC_REBASE);
		
		int tmSDbSave = timer.startExtra();
		OpBlockChain savedParent = dataManager.saveMainBlockchain(blockchain.getParent());
		if(blockchain.getParent() != savedParent) {
			blockchain.changeToEqualParent(savedParent);
		}
		timer.measure(tmSDbSave, ValidationTimer.BLC_SAVE);
		
		int tmCompact = timer.startExtra();
		compact();
		timer.measure(tmCompact, ValidationTimer.BLC_COMPACT);
		
		
		opBlock.putCacheObject(OpObject.F_VALIDATION, timer.getTimes());
		logSystem.logSuccessBlock(opBlock, 
				String.format("New block '%s':%d  is created on top of '%s'. ",
						opBlock.getFullHash(), opBlock.getBlockId(), opBlock.getStringValue(OpBlock.F_PREV_BLOCK_HASH) ));
		return opBlock;
	}

	public synchronized boolean compact() {
		OpBlockChain newParent = dataManager.compact(0, blockchain.getParent(), true);
		if(newParent != blockchain.getParent()) {
			blockchain.changeToEqualParent(newParent);
		}
		return true;
	}
	
	
	public synchronized void clearQueue() {
		// there is no proper clear queue on atomic load
		java.util.Deque<OpOperation> ls = blockchain.getQueueOperations();
		while(!ls.isEmpty()) {
			OpOperation o = ls.getLast();
			// remove from db to not add after restart
			dataManager.removeOperation(o);
			blockchain.removeQueueOperation(o);
		}
		// blockchain = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
	}
	
	public synchronized void bootstrap(String serverName, KeyPair serverLoginKeyPair) throws FailedVerificationException {
		for (String f : BOOTSTRAP_LIST) {
			OpOperation[] lst = formatter.fromJson(
					new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/" + f + ".json")),
					OpOperation[].class);
			if (!OUtils.isEmpty(serverName)) {
				KeyPair kp = null;
				for (OpOperation o : lst) {
					OpOperation op = o;
					if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
						if (kp == null) {
							kp = serverLoginKeyPair;
						}
						op.setSignedBy(serverName);
						op = generateHashAndSign(op, kp);
					}
					addOperation(op);
				}
			}
		}
	}
	
	public synchronized boolean revertSuperblock() throws FailedVerificationException {
		
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		if(blockchain.getParent() == null) {
			return false;
		}
		
		OpBlockChain parent = blockchain.getParent();
		if(parent.isDbAccessed()) {
			OpBlockChain newParent = dataManager.unloadSuperblockFromDB(parent);
			return blockchain.changeToEqualParent(newParent);
		} else {
			OpBlockChain blc = new OpBlockChain(blockchain.getParent().getParent(), blockchain.getRules());
			OpBlockChain pnt = blockchain.getParent();
			List<OpBlock> lst = new ArrayList<OpBlock>(pnt.getSuperblockFullBlocks());
			Collections.reverse(lst);
			for (OpBlock bl : lst) {
				for (OpOperation u : bl.getOperations()) {
					if (!blc.addOperation(u)) {
						return false;
					}
				}
				dataManager.removeFullBlock(bl);
			}
			for (OpOperation o : blockchain.getQueueOperations()) {
				if (!blc.addOperation(o)) {
					return false;
				}
			}
			blockchain = blc;
			String msg = String.format("Revert superblock from '%s:%d' to '%s:%d'", 
					parent.getLastBlockFullHash(), parent.getLastBlockId(), blockchain.getLastBlockFullHash(), blockchain.getLastBlockId());
			logSystem.logSuccessBlock(blockchain.getLastBlockHeader(), msg);
		}
		return true;
	}
	
	public OpBlockChain getBlockchain() {
		return blockchain == null ? OpBlockChain.NULL : blockchain;
	}

	public synchronized void init(MetadataDb metadataDB, OpBlockChain initBlockchain) {
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
		if(blockchain.getStatus() == OpBlockChain.LOCKED_BY_USER) {
			blockchain.unlockByUser();
			return true;
		}
		return false;
	}
	
	public synchronized boolean pauseBlockCreation() {
		if(blockchain.getStatus() == OpBlockChain.UNLOCKED) {
			blockchain.lockByUser();
			return true;
		}
		return false;
	}
	
	public String getCurrentState() {
		if(blockchain.getStatus() == OpBlockChain.UNLOCKED) {
			return "READY";
		} else if(blockchain.getStatus() == OpBlockChain.LOCKED_STATE) {
			return "LOCKED";
		} else if(blockchain.getStatus() == OpBlockChain.LOCKED_OP_IN_PROGRESS) {
			return "OP_IN_PROGRESS";
		}
		return "ERROR";
	}
	
	public boolean isBlockchainPaused() {
		return blockchain.getStatus() != OpBlockChain.UNLOCKED;
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
