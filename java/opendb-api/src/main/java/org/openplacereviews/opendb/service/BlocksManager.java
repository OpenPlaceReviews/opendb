package org.openplacereviews.opendb.service;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.openplacereviews.opendb.ops.auth.SignUpOperation;
import org.openplacereviews.opendb.service.LogOperationService.OperationStatus;
import org.openplacereviews.opendb.service.OpenDBUsersRegistry.ActiveUsersContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;


@Service
public class BlocksManager {

	@Autowired
	public OperationsQueueManager queue;
	
	@Autowired
	public OperationsRegistry registry;
	
	@Autowired
	public LogOperationService logSystem;
	
	@Autowired
	public OpenDBUsersRegistry usersRegistry;
	
	@Autowired
    private JsonFormatter formatter;
	
	@Autowired
	public JdbcTemplate jdbcTemplate;

	public static final int MAX_BLOCK_SIZE = 1000;
	
	public static final int MAX_BLOCK_SIZE_MB = 1 << 20;
	
	public static final int BLOCK_VERSION = 1;
	
	private OpDefinitionBean currentTx;
	private OpBlock currentBlock;
	private BlockchainState currentState = BlockchainState.BLOCKCHAIN_INIT;
	
	
	private OpBlock prevOpBlock = new OpBlock();
	private String serverUser;
	private String serverPrivateKey;
	private KeyPair serverKeyPair;
	private String blockCreationDetails = "";
	private long blockExtra = 0;
	
	public BlocksManager() {
		init();
	}
	
	public enum BlockchainState {
		BLOCKCHAIN_INIT,
		BLOCKCHAIN_READY,
		BLOCKCHAIN_PAUSED,
		BLOCKCHAIN_IN_PROGRESS_BLOCK_EXEC,
		BLOCKCHAIN_FAILED_BLOCK_EXEC
	}
	
	

	public String getServerPrivateKey() {
		return serverPrivateKey;
	}
	
	public String getServerUser() {
		return serverUser;
	}
	
	public KeyPair getServerLoginKeyPair(ActiveUsersContext users) throws FailedVerificationException {
		if(serverUser == null) {
			return null;
		}
		return users.getLoginKeyPair(serverUser, serverPrivateKey);
	}
	
	
	public synchronized String createBlock() {
		if (this.currentState != BlockchainState.BLOCKCHAIN_READY) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		this.currentState = BlockchainState.BLOCKCHAIN_IN_PROGRESS_BLOCK_EXEC;
		try {
			OpBlock bl = new OpBlock();
			currentBlock = bl;
			List<OpDefinitionBean> candidates = bl.getOperations();
			ConcurrentLinkedQueue<OpDefinitionBean> q = queue.getOperationsQueue();
			ActiveUsersContext users = pickupOpsFromQueue(candidates, q, false);
			return executeBlock(bl, users, false);
		} catch (RuntimeException e) {
			this.currentState = BlockchainState.BLOCKCHAIN_FAILED_BLOCK_EXEC;
			throw e;
		} finally {
			if (currentState == BlockchainState.BLOCKCHAIN_IN_PROGRESS_BLOCK_EXEC) {
				this.currentState = BlockchainState.BLOCKCHAIN_READY;
			}
		}
	}
	
	public synchronized String replicateBlock(OpBlock remoteBlock) {
		if(this.currentState != BlockchainState.BLOCKCHAIN_READY) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		this.currentState = BlockchainState.BLOCKCHAIN_IN_PROGRESS_BLOCK_EXEC;
		try {
			currentBlock = remoteBlock;
			LinkedList<OpDefinitionBean> ops = new LinkedList<>(remoteBlock.getOperations());
			ArrayList<OpDefinitionBean> cand = new ArrayList<>();
			ActiveUsersContext users = pickupOpsFromQueue(cand, ops, true);
			if(ops.size() != cand.size()) {
				throw new RuntimeException("The block could not validate all transactions included in it");
			}
			return executeBlock(remoteBlock, users, true);
		} catch (RuntimeException e) {
			this.currentState = BlockchainState.BLOCKCHAIN_FAILED_BLOCK_EXEC;
			throw e;
		} finally {
			if(currentState == BlockchainState.BLOCKCHAIN_IN_PROGRESS_BLOCK_EXEC) {
				this.currentState = BlockchainState.BLOCKCHAIN_READY;
			}
		}
	}
	
	public synchronized void init() {
		serverUser = System.getenv("OPENDB_SIGN_LOGIN");
		serverPrivateKey = System.getenv("OPENDB_SIGN_PK");
		currentState = BlockchainState.BLOCKCHAIN_READY;
	}
	
	
	public synchronized boolean resumeBlockCreation() {
		if(currentState == BlockchainState.BLOCKCHAIN_PAUSED) {
			currentState = BlockchainState.BLOCKCHAIN_READY;
			return true;
		}
		return false;
	}
	
	public synchronized boolean pauseBlockCreation() {
		if(currentState == BlockchainState.BLOCKCHAIN_READY) {
			currentState = BlockchainState.BLOCKCHAIN_PAUSED;
			return true;
		}
		return false;
	}
	
	public BlockchainState getCurrentState() {
		return currentState;
	}
	
	public OpBlock getCurrentBlock() {
		return currentBlock;
	}
	
	public OpDefinitionBean getCurrentTx() {
		return currentTx;
	}
	
	private ActiveUsersContext pickupOpsFromQueue(List<OpDefinitionBean> candidates,
			Collection<OpDefinitionBean> q, boolean exceptionOnFail) {
		int size = 0;
		ActiveUsersContext au = new OpenDBUsersRegistry.ActiveUsersContext(usersRegistry.getBlockUsers());
		Map<String, Set<String>> authTxDependencies = new HashMap<String, Set<String>>();
		for (OpDefinitionBean o : q) {
			int l = formatter.toJson(o).length();
			String validMsg = null; 
			Exception ex = null;
			try {
				if(!usersRegistry.validateSignatures(au, o)) {
					validMsg = "not verified";
				}
				if(!usersRegistry.validateHash(o)) {
					validMsg = "hash is not valid";
				}
				if(!usersRegistry.validateSignatureHash(o)) {
					validMsg = "signature hash is not valid";
				}
				validMsg = usersRegistry.validateRoles(au, o);
			} catch (Exception e) {
				ex = e;
			}
			if(ex != null) {
				logSystem.logOperation(OperationStatus.FAILED_PREPARE, o, String.format("Failed to verify operation signature: %s", validMsg),
						exceptionOnFail, ex);
				continue;
			}
			
			if(l > MAX_BLOCK_SIZE_MB / 2) {
				logSystem.logOperation(OperationStatus.FAILED_PREPARE, o, String.format("Operation discarded due to size limit %d", l), exceptionOnFail);
				continue;
			}
			if(size + l > MAX_BLOCK_SIZE) {
				break;
			}
			if(candidates.size() >= MAX_BLOCK_SIZE) {
				break;
			}
			boolean authOp = au.addAuthOperation(o);
			if(authOp) {
				String uname = o.getStringValue(SignUpOperation.F_NAME);
				if(!authTxDependencies.containsKey(uname)) {
					authTxDependencies.put(uname, new LinkedHashSet<String>());
				}
				o.setTransientTxDependencies(new ArrayList<String>(authTxDependencies.get(uname)));
				authTxDependencies.get(uname).add(o.getHash());
			}
			candidates.add(o);
		}
		return au;
	}

	


	private String executeBlock(OpBlock block, ActiveUsersContext users, boolean exceptionOnFail) {
		List<OpenDBOperationExec> operations = prepareOperationCtxToExec(block, exceptionOnFail);
		if(block.blockId < 0) {
			signBlock(block, users);
		}
		validateBlock(block, users);
		
		// here we don't expect any failure or the will be fatal to the system
		executeOperations(block, operations);
		
		// don't keep operations in memory
		prevOpBlock = new OpBlock(block);
		List<OpDefinitionBean> ops = prevOpBlock.getOperations();
		for(OpDefinitionBean o : block.getOperations()) {
			OpDefinitionBean ro = new OpDefinitionBean(o);
			ro.getRawOtherFields().clear();
			ro.putStringValue(OpDefinitionBean.F_HASH, o.getHash());
			ro.putStringValue(OpDefinitionBean.F_NAME, o.getName());
			ops.add(ro);
		}
		queue.removeSuccessfulOps(block);
		return formatter.toJson(prevOpBlock);
	}

	private void validateBlock(OpBlock block, ActiveUsersContext users) {
		if(block.getOperations().size() == 0) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block,
					"Block has no operations to execute", true);
		}
		if(!OUtils.equals(usersRegistry.calculateMerkleTreeHash(block), block.merkleTreeHash)) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block,
					String.format("Failed to validate merkle tree: %s %s", usersRegistry.calculateMerkleTreeHash(block), block.merkleTreeHash), true);
		}
		if(!OUtils.equals(usersRegistry.calculateSigMerkleTreeHash(block), block.sigMerkleTreeHash)) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block,
					String.format("Failed to validate signature merkle tree: %s %s", usersRegistry.calculateMerkleTreeHash(block), block.merkleTreeHash), true);
		}
		if(!OUtils.equals(prevOpBlock.hash, block.previousBlockHash)) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block,
					String.format("Failed to validate previous block hash: %s %s", prevOpBlock.hash, block.previousBlockHash), true);
		}
		if(!OUtils.equals(calculateHash(block), block.hash)) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block,
					String.format("Failed to validate block hash: %s %s", calculateHash(block), block.hash), true);
		}
		if(prevOpBlock.blockId + 1 != block.blockId) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block,
					String.format("Block id doesn't match with previous block id: %d %d", prevOpBlock.blockId, block.blockId), true);
		}
		boolean validateSig = true;
		Exception ex = null;
		try {
			KeyPair pk = users.getPublicLoginKeyPair(block.signedBy);
			byte[] blHash = SecUtils.getHashBytes(block.hash);		
			byte[] signature = SecUtils.decodeSignature(block.signature);
			if(pk != null && SecUtils.validateSignature(pk, blHash, block.signatureAlgo, signature)) {
				validateSig = true;
			} else {
				validateSig = false;
			}
		} catch (FailedVerificationException e) {
			validateSig = false;
			ex = e;
		} catch (RuntimeException e) {
			validateSig = false;
			ex = e;
		}
		if (!validateSig) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block,
					"Block signature doesn't match", true, ex);
		}
	}

	private void signBlock(OpBlock block, ActiveUsersContext users) {
		try {
			block.setDate(System.currentTimeMillis());
			block.blockId = prevOpBlock.blockId + 1;
			block.previousBlockHash = prevOpBlock.hash;
			block.merkleTreeHash = usersRegistry.calculateMerkleTreeHash(block);
			block.sigMerkleTreeHash = usersRegistry.calculateSigMerkleTreeHash(block);
			block.signedBy = serverUser;
			block.version = BLOCK_VERSION;
			block.extra = blockExtra;
			block.details = blockCreationDetails;
			block.hash = calculateHash(block);
			byte[] hashBytes = SecUtils.getHashBytes(block.hash);
			if(serverKeyPair == null) {
				serverKeyPair = users.getLoginKeyPair(serverUser, serverPrivateKey);	
			}
			block.signature = SecUtils.signMessageWithKeyBase64(serverKeyPair, hashBytes, SecUtils.SIG_ALGO_NONE_EC, null);
			block.signatureAlgo = SecUtils.SIG_ALGO_NONE_EC;
		} catch (FailedVerificationException e) {
			logSystem.logBlock(OperationStatus.FAILED_VALIDATE, block, "Failed to sign the block: " + e.getMessage(), true, e);
		}
	}



	private String calculateHash(OpBlock block) {
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		DataOutputStream dous = new DataOutputStream(bs);
		try {
			dous.writeInt(block.version);
			dous.writeInt(block.blockId);
			dous.write(SecUtils.getHashBytes(block.previousBlockHash));
			dous.writeLong(block.getDate());
			dous.write(SecUtils.getHashBytes(block.merkleTreeHash));
			dous.write(SecUtils.getHashBytes(block.sigMerkleTreeHash));
			dous.writeLong(block.extra);
			if(!OUtils.isEmpty(block.details)) {
				dous.write(block.details.getBytes("UTF-8"));
			}
			dous.write(block.signedBy.getBytes("UTF-8"));
			dous.flush();
			return SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, bs.toByteArray());
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}



	private void executeOperations(OpBlock block, List<OpenDBOperationExec> operations) {
		for (OpenDBOperationExec o : operations) {
			currentTx = o.getDefinition();
			boolean execute = false;
			RuntimeException err = null;
			try {
				execute = o.execute(jdbcTemplate);
			} catch (RuntimeException e) {
				err = e;
			}
			if (!execute) {
				logSystem.logOperation(OperationStatus.FAILED_EXECUTE, o.getDefinition(), String.format(
						"Operations failed to execute %s %s", o.getDefinition().getOperationId(), o.getDefinition()
								.getHash()), true, err);
				// not reachable code
				throw new IllegalStateException(err);
			} else {
				usersRegistry.getBlockUsers().addAuthOperation(o.getDefinition());
				logSystem.logOperation(OperationStatus.EXECUTED, o.getDefinition(), "OK", false);
			}
		}
	}


	private List<OpenDBOperationExec> prepareOperationCtxToExec(OpBlock block, boolean exceptionOnFail) {
		List<OpenDBOperationExec> operations = new ArrayList<OpenDBOperationExec>();
		Map<String, OpDefinitionBean> executedTx = new TreeMap<String, OpDefinitionBean>();
		Iterator<OpDefinitionBean> it = block.getOperations().iterator();
		while(it.hasNext()) {
			OpDefinitionBean def = it.next();
			OpenDBOperationExec op = registry.createOperation(def);
			boolean valid = false;
			Exception ex = null;
			try {
				valid = op != null && op.prepare(def);
			} catch (Exception e) {
				ex = e;
			}
			if(valid) { 
				boolean allDeps = checkAllDependencies(executedTx, def.getTransientTxDependencies());
				if(allDeps) {
					allDeps = checkAllDependencies(executedTx, def.getStringList(OpDefinitionBean.F_DEPENDENCIES));
				}
				if(!allDeps) {
					logSystem.logOperation(OperationStatus.FAILED_DEPENDENCIES, def, 
							String.format("Operations has dependencies there were not executed yet", def.getHash()), exceptionOnFail);
					valid = false;
				}
				if(executedTx.containsKey(def.getHash())) {
					logSystem.logOperation(OperationStatus.FAILED_EXECUTE, def, 
							String.format("Operations has duplicate hash in same block: %s", def.getHash()), exceptionOnFail);
					valid = false;
				}
			} else {
				logSystem.logOperation(OperationStatus.FAILED_PREPARE, def,
						String.format("Operation %s %s couldn't be validated for execution", def.getOperationId(), def.getHash()) , ex);
			}
			if(valid) {
				operations.add(op);
				executedTx.put(def.getHash(), def);
			} else {
				it.remove();
			}
		}
		return operations;
	}


	private boolean checkAllDependencies(Map<String, OpDefinitionBean> executedTx, List<String> dp) {
		if (dp != null) {
			for (String d : dp) {
				if (!executedTx.containsKey(d)) {
					return false;
				}
			}
		}
		return true;
	}
	


	
}
