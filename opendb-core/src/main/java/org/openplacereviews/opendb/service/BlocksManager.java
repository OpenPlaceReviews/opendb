package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.MgmtController;
import org.openplacereviews.opendb.dto.HistoryDTO;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyPair;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpBlockChain.*;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_VOTE;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_VOTING;
import static org.openplacereviews.opendb.ops.OpOperation.F_EDITED_OBJECT;


@Service
public class BlocksManager {

	public static final String BOOT_STD_OPS_DEFINTIONS = "std-ops-defintions";
	public static final String BOOT_STD_ROLES = "std-roles";
	public static final String BOOT_STD_VALIDATION = "std-validations";

	private static final String HISTORY_BY_USER = "user";
	private static final String HISTORY_BY_OBJECT = "object";
	private static final String HISTORY_BY_TYPE = "type";


	protected static final Log LOGGER = LogFactory.getLog(BlocksManager.class);
	
	@Autowired
	private LogOperationService logSystem;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	private DBConsensusManager dataManager;

	@Autowired
	private IPFSFileManager extResourceService;

	protected List<String> bootstrapList = new ArrayList<>();
	
	@Value("${opendb.replicate.url}")
	private String replicateUrl;
	
	@Value("${opendb.mgmt.user}")
	private String serverUser;
	
	@Value("${opendb.mgmt.privateKey}")
	private String serverPrivateKey;
	
	@Value("${opendb.mgmt.publicKey}")
	private String serverPublicKey;
	private KeyPair serverKeyPair;
	
	private BlockchainMgmtStatus mgmtStatus = BlockchainMgmtStatus.BLOCK_CREATION; 
	
	private OpBlockChain blockchain; 
	
	private enum BlockchainMgmtStatus {
		BLOCK_CREATION,
		REPLICATION,
		NONE,
	}
	
	public String getServerPrivateKey() {
		return serverPrivateKey;
	}
	
	public String getServerUser() {
		return serverUser;
	}
	
	public KeyPair getServerLoginKeyPair() {
		return serverKeyPair;
	}
	
	public boolean isBlockCreationOn() {
		return this.mgmtStatus == BlockchainMgmtStatus.BLOCK_CREATION;
	}
	
	public boolean isReplicateOn() {
		return this.mgmtStatus == BlockchainMgmtStatus.REPLICATION && !OUtils.isEmpty(replicateUrl);
	}
	
	public String getReplicateUrl() {
		return replicateUrl;
	}

	public void getHistory(HistoryDTO.HistoryObjectRequest historyObjectRequest) {
		switch (historyObjectRequest.historyType) {
			case HISTORY_BY_USER:
				dataManager.getHistoryForUser(historyObjectRequest);
				break;
			case HISTORY_BY_OBJECT:
				dataManager.getHistoryForObject(historyObjectRequest);
				break;
			case HISTORY_BY_TYPE:
				dataManager.getHistoryForType(historyObjectRequest);
				break;
		}
	}
	
	public synchronized void setReplicateOn(boolean on) {
		if(on && this.mgmtStatus == BlockchainMgmtStatus.NONE) {
			this.mgmtStatus = BlockchainMgmtStatus.REPLICATION;
		} else if(!on && this.mgmtStatus == BlockchainMgmtStatus.REPLICATION) {
			this.mgmtStatus = BlockchainMgmtStatus.NONE;
		}
	}
	
	public synchronized void setBlockCreationOn(boolean on) {
		if(on && this.mgmtStatus == BlockchainMgmtStatus.NONE) {
			this.mgmtStatus = BlockchainMgmtStatus.BLOCK_CREATION;
		} else if(!on && this.mgmtStatus == BlockchainMgmtStatus.BLOCK_CREATION) {
			this.mgmtStatus = BlockchainMgmtStatus.NONE;
		}
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
	
	public synchronized boolean unlockBlockchain() {
		if(blockchain.getStatus() == OpBlockChain.LOCKED_BY_USER) {
			blockchain.unlockByUser();
			return true;
		}
		return false;
	}
	
	public synchronized boolean lockBlockchain() {
		if(blockchain.getStatus() == OpBlockChain.UNLOCKED) {
			blockchain.lockByUser();
			return true;
		}
		return false;
	}
	
	public synchronized boolean validateOperation(OpOperation op) {
		if(blockchain == null) {
			return false;
		}
		return blockchain.validateOperation(op);
	}
	
	public synchronized boolean removeOrphanedBlock(String blockHash) {
		OpBlock block = dataManager.getOrphanedBlocks().get(blockHash);
		if(block != null) {
			return dataManager.removeFullBlock(block);
		}
		return false;
	}

	// TODO -> check sys.vote on voting for each user
	// TODO -> sys.voting can be called only by admin?
	// TODO -> change history struct !!! -> update admin.html
	// TODO -> add admin tab for view voting process
	// TODO -> add user tab for voting
	// TODO -> write tests for voting process
	public synchronized boolean addOperation(OpOperation op) throws FailedVerificationException {
		if(blockchain == null) {
			return false;
		}
		op.makeImmutable();
		if (!createVoteObject(op)) {
			throw new IllegalArgumentException("Voting object was not created");
		}
		if (!createFinishedVotingObject(op)) {
			throw new IllegalArgumentException("Voting cannot be finished");
		}
		boolean existing = dataManager.validateExistingOperation(op);
		if (!op.hasEdited() || op.getType().equals(OP_VOTE)) {
			boolean added = blockchain.addOperation(op);
			if(!existing) {
				dataManager.insertOperation(op, OpOperation.Status.ACTIVE);
			}
			return added;
		}
		// all 3 methods in synchronized block, so it is almost guaranteed insertOperation won't fail
		// or that operation will be lost in queue and system needs to be restarted
		if(!existing) {
			dataManager.insertOperation(op, OpOperation.Status.VOTING);
		}
		return true;
	}

	private boolean createVoteObject(OpOperation op) throws FailedVerificationException {
		if (op.hasEdited() && !op.getType().equals(OP_VOTE)) {
			OpOperation opOperation = new OpOperation();
			opOperation.setType(OP_VOTE);
			opOperation.setSignedBy(serverUser);

			OpObject opObject = new OpObject();
			opObject.putObjectValue(F_EDITED_OBJECT, op.getEdited());
			opObject.putObjectValue("votes", 0L);
			opObject.setId(op.getRawHash());
			for (String objId : op.getEdited().get(0).getId()) {
				opObject.addOrSetStringValue(OpObject.F_ID, objId);
			}
			opOperation.addCreated(opObject);
			generateHashAndSign(opOperation, serverKeyPair);

			opOperation.makeImmutable();
			boolean existing = dataManager.validateExistingOperation(opOperation);
			boolean added = blockchain.addOperation(opOperation);
			if (!existing) {
				dataManager.insertOperation(opOperation, OpOperation.Status.ACTIVE);
			}

			return added;
		}

		return true;
	}

	private boolean createFinishedVotingObject(OpOperation op) {
		if (op.getType().equals(OP_VOTING)) {
			Map<String, List<String>> refObjectList = op.getRef();
			if (refObjectList == null) {
				return false;
			}

			for (Map.Entry<String, List<String>> e : refObjectList.entrySet()) {
				List<String> refObjName = e.getValue();
				if (refObjName.size() > 1) {
					// type is necessary
					String objType = refObjName.get(0);
					List<String> refKey = refObjName.subList(1, refObjName.size());
					OpObject refObject = blockchain.getObjectByName(objType, refKey);
					blockchain.validateVotingRefObject(op, refObject, refKey);

					OpOperation opOperation = dataManager.getOperationByHash(SecUtils.HASH_SHA256 + ":" + refObject.getId().get(0));
					opOperation.makeImmutable();
					blockchain.addOperation(opOperation);
					dataManager.updateOperationStatus(opOperation, OpOperation.Status.ACTIVE);
				}
			}
		}

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
		
		List<OpOperation> candidates = pickupOpsFromQueue(blockchain.getQueueOperations());
		
		int tmAddOps = timer.startExtra();
		OpBlockChain blc = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
		for (OpOperation o : candidates) {
			if(!blc.addOperation(o)) {
				return null;
			}
		}
		timer.measure(tmAddOps, ValidationTimer.BLC_ADD_OPERATIONS);
		
		extResourceService.processOperations(candidates);
		timer.measure(tmAddOps, ValidationTimer.BLC_PROCESS_RESOURCES);

		int tmNewBlock = timer.startExtra();
		OpBlock opBlock = blc.createBlock(serverUser, serverKeyPair);
		if(opBlock == null) {
			return null;
		}

		timer.measure(tmNewBlock, ValidationTimer.BLC_NEW_BLOCK);
		
		return replicateValidBlock(timer, blc, opBlock);
	}

	private OpBlock replicateValidBlock(ValidationTimer timer, OpBlockChain blockChain, OpBlock opBlock) {
		// insert block could fail if hash is duplicated but it won't hurt the system
		int tmDbSave = timer.startExtra();
		dataManager.insertBlock(opBlock);
		saveHistoryForBlockOperations(opBlock);
		timer.measure(tmDbSave, ValidationTimer.BLC_BLOCK_SAVE);
		
		// change only after block is inserted into db
		int tmRebase = timer.startExtra();
		boolean changeParent = blockchain.rebaseOperations(blockChain);
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

	private void saveHistoryForBlockOperations(OpBlock opBlock) {
		Date date = new Date(opBlock.getDate(OpBlock.F_DATE));
		Map<List<String>, OpObject> lastOriginObjects = new HashMap<>();
		for (OpOperation o : opBlock.getOperations()) {
			List<OpObject> newEditedObjects = new LinkedList<>();
			if (o.hasEdited()) {
				for (OpObject opObject : o.getEdited()) {
					OpObject lastOriginObject = getOriginOpObject(lastOriginObjects, o, opObject);
					OpObject newObject = new OpObject(lastOriginObject);

					Map<String, Object> changedMap = opObject.getChangedEditFields();
					for (Map.Entry<String, Object> e : changedMap.entrySet()) {
						// evaluate changes for new object
						String fieldExpr = e.getKey();
						Object op = e.getValue();
						String opId = op.toString();
						Object opValue = null;
						if (op instanceof Map) {
							Map.Entry<String, Object> ee = ((Map<String, Object>) op).entrySet().iterator().next();
							opId = ee.getKey();
							opValue = ee.getValue();
						}

						if (OP_CHANGE_DELETE.equals(opId)) {
							newObject.setFieldByExpr(fieldExpr, null);
						} else if (OP_CHANGE_SET.equals(opId)) {
							newObject.setFieldByExpr(fieldExpr, opValue);
						} else if (OP_CHANGE_APPEND.equals(opId)) {
							Object oldObject = newObject.getFieldByExpr(fieldExpr);
							if (oldObject == null) {
								List<Object> args = new ArrayList<>(Collections.singletonList(opValue));
								newObject.setFieldByExpr(fieldExpr, args);
							} else if (oldObject instanceof List) {
								((List) oldObject).add(opValue);
							}
						} else if (OP_CHANGE_INCREMENT.equals(opId)) {
							Object oldObject = newObject.getFieldByExpr(fieldExpr);
							if (oldObject == null) {
								newObject.setFieldByExpr(fieldExpr, 1);
							} else if (oldObject instanceof Number) {
								newObject.setFieldByExpr(fieldExpr, (((Long) oldObject) + 1));
							}
						}
					}

					newEditedObjects.add(newObject);
					lastOriginObjects.put(opObject.getId(), newObject);
				}
			}
			dataManager.saveHistoryForObjects(o, date, newEditedObjects);
		}
	}

	private OpObject getOriginOpObject(Map<List<String>, OpObject> lastOriginObjects, OpOperation o, OpObject opObject) {
		OpObject lastOriginObject = lastOriginObjects.get(opObject.getId());
		if (lastOriginObject == null) {
			lastOriginObject = dataManager.getLastOriginObjectFromHistory(opObject.getId());
		}

		if (lastOriginObject == null) {
			lastOriginObject = blockchain.getObjectByName(o.getType(), opObject.getId());
		}
		return lastOriginObject;
	}

	public synchronized boolean compact() {
		OpBlockChain newParent = dataManager.compact(0, blockchain.getParent(), true);
		if(newParent != blockchain.getParent()) {
			blockchain.changeToEqualParent(newParent);
		}
		return true;
	}
	
	public synchronized boolean clearQueue() {
		TreeSet<String> set = new TreeSet<>(); 
		for(OpOperation o: blockchain.getQueueOperations()) {
			set.add(o.getRawHash());
		}
		boolean cleared = blockchain.removeAllQueueOperations();
		if(!cleared) {
			return false;
		}
		return dataManager.removeOperations(set) == set.size();
		// blockchain = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
	}
	
	private Reader readerFromUrl(String url) throws IOException {
		return new InputStreamReader(new URL(url).openStream());
	}
	
	public synchronized boolean replicate() {
		if(isReplicateOn()) {
			try {
				String from = blockchain.getLastBlockRawHash();
				OpBlock[] replicateBlockHeaders = formatter.fromJson(
						readerFromUrl(replicateUrl + "blocks?from=" + from), 
								OpBlock[].class);
				LinkedList<OpBlock> headersToReplicate = new LinkedList<OpBlock>(Arrays.asList(replicateBlockHeaders));
				if(!OUtils.isEmpty(from) && headersToReplicate.size() > 0) {
					if(!OUtils.equals(headersToReplicate.peekFirst().getRawHash(), from)) {
						logSystem.logError(headersToReplicate.peekFirst(), ErrorType.MGMT_REPLICATION_BLOCK_CONFLICTS, 
								ErrorType.MGMT_REPLICATION_BLOCK_CONFLICTS.getErrorFormat(
										headersToReplicate.peekFirst().getRawHash(), from, headersToReplicate), null);
						return false;
					} else {
						headersToReplicate.removeFirst();	
					}
				}
				for(OpBlock header : headersToReplicate) {
					OpBlock fullBlock = downloadBlock(header);
					if(fullBlock == null) {
						logSystem.logError(header, ErrorType.MGMT_REPLICATION_BLOCK_DOWNLOAD_FAILED, 
								ErrorType.MGMT_REPLICATION_BLOCK_DOWNLOAD_FAILED.getErrorFormat(header.getRawHash()), null);
						return false;
					}
					fullBlock.makeImmutable();
					for (OpOperation o : fullBlock.getOperations()) {
						if (!dataManager.validateExistingOperation(o)) {
							dataManager.insertOperation(o, OpOperation.Status.ACTIVE);
						}
					}
					replicateOneBlock(fullBlock);
				}
				return true;
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
				logSystem.logError(null, ErrorType.MGMT_REPLICATION_IO_FAILED, "Failed to replicate from " + replicateUrl, e);
			}
		}
		return false;
	}

	private OpBlock downloadBlock(OpBlock header) throws MalformedURLException, IOException {
		URL downloadByHash = new URL(replicateUrl + "block-by-hash?hash=" + header.getRawHash());
		OpBlock res = formatter.fromJson(new InputStreamReader(downloadByHash.openStream()), OpBlock.class);
		if(res.getBlockId() == -1) {
			return null;
		}
		return res;
	}
	
	public synchronized boolean replicateOneBlock(OpBlock block) {
		ValidationTimer timer = new ValidationTimer();
		timer.start();
		OpBlockChain blc = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
		OpBlock res;
		res = blc.replicateBlock(block);
		if(res == null) {
			return false;
		}
		res = replicateValidBlock(timer, blc, res);
		if(res == null) {
			return false;
		}
		return true;
	}
	
	public synchronized Set<String> removeQueueOperations(Set<String> operationsToDelete) {
		Set<String> deleted;
		try {
			deleted = blockchain.removeQueueOperations(operationsToDelete);
		} catch (RuntimeException e) {
			// handle non last operations - slow method
			deleted = new TreeSet<String>();
			OpBlockChain blc = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
			for (OpOperation o : blockchain.getQueueOperations()) {
				if (!operationsToDelete.contains(o.getRawHash())) {
					if (!blc.addOperation(o)) {
						return null;
					}
				} else {
					deleted.add(o.getRawHash());
				}
			}
			blockchain = blc; 
		}
		dataManager.removeOperations(deleted);
		return deleted;
	}
	
	public synchronized void bootstrap(String serverName, KeyPair serverLoginKeyPair) throws FailedVerificationException {
		for (String f : bootstrapList) {
			OpOperation[] lst = formatter.fromJson(
					new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/" + f + ".json")),
					OpOperation[].class);
			if (!OUtils.isEmpty(serverName)) {
				for (OpOperation o : lst) {
					OpOperation op = o;
					if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
						op.setSignedBy(serverName);
						op = generateHashAndSign(op, serverLoginKeyPair);
					}
					addOperation(op);
				}
			}
		}
	}
	
	public synchronized boolean revertOneBlock() throws FailedVerificationException {
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		if (blockchain.getLastBlockRawHash().equals("")) {
			return false;
		}
		if (blockchain.getParent().getSuperblockSize() == 1 || blockchain.getParent().isDbAccessed()) {
			return revertSuperblock();
		}

		OpBlockChain newParent = new OpBlockChain(blockchain.getParent().getParent(), blockchain.getRules());
		Deque<OpBlock> superblockFullBlocks = blockchain.getParent().getSuperblockFullBlocks();
		Iterator<OpBlock> it = superblockFullBlocks.descendingIterator();
		if (!it.hasNext()) {
			return false;
		}
		OpBlock blockToRevert = null;
		while (it.hasNext()) {
			blockToRevert = it.next();
			if (it.hasNext()) {
				newParent.replicateBlock(blockToRevert);
			}
		}
		OpBlockChain blc = new OpBlockChain(newParent, blockchain.getRules());
		for (OpOperation o : blockToRevert.getOperations()) {
			if (!blc.addOperation(o)) {
				return false;
			}
		}
		for (OpOperation o : blockchain.getQueueOperations()) {
			if (!blc.addOperation(o)) {
				return false;
			}
		}
		dataManager.removeFullBlock(blockToRevert);
		blockchain = blc;
		String msg = String.format("Revert block '%s:%d'", 
				blockToRevert.getRawHash(), blockToRevert.getBlockId());
		logSystem.logSuccessBlock(blockToRevert, msg);
		return true;
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
	
	public Map<String, OpBlock> getOrphanedBlocks() {
		return dataManager.getOrphanedBlocks();
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

	public List<String> getBootstrapList() {
		return bootstrapList;
	}

	public void setBootstrapList(List<String> bootstrapList) {
		this.bootstrapList = bootstrapList;
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
