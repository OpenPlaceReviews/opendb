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
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;


@Service
public class BlocksManager {
	protected static final Log LOGGER = LogFactory.getLog(BlocksManager.class);
	
	@Autowired
	private OperationsRegistry registry;
	
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
	
	private volatile BlockchainState currentState = BlockchainState.BLOCKCHAIN_INIT;
	
	public enum BlockchainState {
		BLOCKCHAIN_INIT,
		BLOCKCHAIN_READY,
		BLOCKCHAIN_PAUSED
	}
	
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
	
	public void addOperation(OpOperation op) {
		// TODO LOG success or failure (create queue of failed) ops
		blockchain.addOperation(op, blockchainRules);
	}
	
	public void clearQueue() {
		blockchain = new OpBlockChain(blockchain.getParent());
	}
	
	public OpBlock createBlock() throws FailedVerificationException {
		if (this.currentState != BlockchainState.BLOCKCHAIN_READY) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		// TODO add logging for failures like addOperation, createBlock, changeParent
		List<OpOperation> candidates = pickupOpsFromQueue(blockchain.getOperations());
		OpBlockChain blc = new OpBlockChain(blockchain.getParent());
		for (OpOperation o : candidates) {
			if(!blc.addOperation(o, blockchainRules)) {
				return null;
			}
		}
		OpBlock opBlock = blc.createBlock(blockchainRules);
		if(opBlock == null) {
			return null;
		}
		boolean changeParent = blockchain.changeParent(blc);
		if(!changeParent) {
			return null;
		}
		blc.compact();
		return opBlock;
	}
	
	public OpBlockChain getBlockchain() {
		return blockchain;
	}

	public void init(MetadataDb metadataDB) {
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
		currentState = BlockchainState.BLOCKCHAIN_READY;
		LOGGER.info("+++ Blockchain is inititialized. " + msg);
	}
	
	
	public boolean resumeBlockCreation() {
		if(currentState == BlockchainState.BLOCKCHAIN_PAUSED) {
			currentState = BlockchainState.BLOCKCHAIN_READY;
			return true;
		}
		return false;
	}
	
	public boolean pauseBlockCreation() {
		if(currentState == BlockchainState.BLOCKCHAIN_READY) {
			currentState = BlockchainState.BLOCKCHAIN_PAUSED;
			return true;
		}
		return false;
	}
	
	public BlockchainState getCurrentState() {
		return currentState;
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

	

	
}
