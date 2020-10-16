package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.MgmtController;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.DeletedObjectCtx;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;
import org.openplacereviews.opendb.ops.PerformanceMetrics.Metric;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.service.SettingsManager.BlockSource;
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

@Service
public class BlocksManager {

	public static final String BOOT_STD_OPS_DEFINTIONS = "std-ops-defintions";
	public static final String BOOT_STD_ROLES = "std-roles";
	public static final String BOOT_STD_VALIDATION = "std-validations";

	protected static final Log LOGGER = LogFactory.getLog(BlocksManager.class);
	
	@Autowired
	private LogOperationService logSystem;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	private DBConsensusManager dataManager;

	@Autowired
	private HistoryManager historyManager;

	@Autowired
	private IPFSFileManager extResourceService;

	@Autowired
	private SettingsManager settingsManager;

	protected List<String> bootstrapList = new ArrayList<>();
	
	@Value("${opendb.mgmt.user}")
	private String serverUser;
	
	@Value("${opendb.mgmt.privateKey}")
	private String serverPrivateKey;
	
	@Value("${opendb.mgmt.publicKey}")
	private String serverPublicKey;
	
	private KeyPair serverKeyPair;
	
	private OpBlockChain blockchain;
	
	private String statusDescription = "";
	
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
		return settingsManager.OPENDB_BLOCKCHAIN_STATUS.get() == BlockSource.CREATE;
	}
	
	public boolean isReplicateOn() {
		return settingsManager.OPENDB_BLOCKCHAIN_STATUS.get() == BlockSource.REPLICATION && !OUtils.isEmpty(getReplicateUrl());
	}
	
	public String getReplicateUrl() {
		return settingsManager.OPENDB_REPLICATE_URL.get();
	}
	
	public synchronized void setReplicateOn(boolean on) {
		BlockSource bs = settingsManager.OPENDB_BLOCKCHAIN_STATUS.get();
		if (on && bs == BlockSource.NONE) {
			settingsManager.OPENDB_BLOCKCHAIN_STATUS.set(BlockSource.REPLICATION);
		} else if (!on && bs == BlockSource.REPLICATION) {
			settingsManager.OPENDB_BLOCKCHAIN_STATUS.set(BlockSource.NONE);
		}
	}
	
	public synchronized void setBlockCreationOn(boolean on) {
		BlockSource bs = settingsManager.OPENDB_BLOCKCHAIN_STATUS.get();
		if(on && bs == BlockSource.NONE) {
			settingsManager.OPENDB_BLOCKCHAIN_STATUS.set(BlockSource.CREATE);
		} else if(!on && bs == BlockSource.CREATE) {
			settingsManager.OPENDB_BLOCKCHAIN_STATUS.set(BlockSource.NONE);
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
			statusDescription = "";
			return true;
		}
		return false;
	}

	public synchronized boolean lockBlockchain(String msg) {
		if(blockchain.getStatus() == OpBlockChain.UNLOCKED) {
			blockchain.lockByUser();
			statusDescription = msg;
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

	public synchronized boolean addOperation(OpOperation op) {
		if (blockchain == null) {
			return false;
		}
		Metric m = mBlockAddOpp.start();
		op.makeImmutable();
		boolean existing = dataManager.validateExistingOperation(op);
		if (!existing) {
			dataManager.insertOperation(op);
		}
		boolean added = false;
		try {
			added = blockchain.addOperation(op);
		} finally {
			if (!added && !existing) {
				// we need to remove operations cause we don't support orphaned ops and ops will break the system 
				dataManager.removeOperations(Collections.singleton(op.getHash()));
			}
			m.capture();
		}
		return added;
	}

	public synchronized OpBlock createBlock() throws FailedVerificationException {
		return createBlock(0);
	}
	
	public synchronized OpBlock createBlock(double minCapacity) throws FailedVerificationException {
		// should be changed synchronized in future:
		// This method doesn't need to be full synchronized cause it could block during compacting or any other operation adding ops
		if(blockchain.getQueueOperations().isEmpty()) {
			return null;
		}
		if (OpBlockChain.UNLOCKED != blockchain.getStatus()) {
			throw new IllegalStateException("Blockchain is not ready to create block");
		}
		Metric mt = mBlockCreate.start();		
		List<OpOperation> candidates = pickupOpsFromQueue(minCapacity, blockchain.getQueueOperations());
		if(candidates == null) {
			mt.capture();
			return null;
		}
		
		Metric m = mBlockCreateAddOps.start();
		OpBlockChain blc = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
		DeletedObjectCtx hctx = new DeletedObjectCtx();
		for (OpOperation o : candidates) {
			if(!blc.addOperation(o, hctx)) {
				return null;
			}
		}
		m.capture();
		m = mBlockCreateExtResources.start();
		extResourceService.processOperations(candidates);
		m.capture();
		
		m = mBlockCreateValidate.start();
		OpBlock opBlock = blc.createBlock(serverUser, serverKeyPair);
		m.capture();
		if(opBlock == null) {
			return null;
		}

		mt.capture();
		return replicateValidBlock(blc, opBlock, hctx);
	}

	private OpBlock replicateValidBlock(OpBlockChain blockChain, OpBlock opBlock, DeletedObjectCtx hctx) {
		Metric pm = mBlockReplicate.start();
		// insert block could fail if hash is duplicated but it won't hurt the system
		Metric m = mBlockSaveBlock.start();
		dataManager.insertBlock(opBlock);
		m.capture();

		
		m = mBlockSaveHistory.start();
		historyManager.saveHistoryForBlockOperations(opBlock, hctx);
		m.capture();
		
		// change only after block is inserted into db
		m = mBlockRebase.start();
		boolean changeParent = this.blockchain.rebaseOperations(blockChain);
		if(!changeParent) {
			return null;
		}
		m.capture();
		compact();
		OpBlock header = OpBlock.createHeader(opBlock, blockchain.getRules());
		logSystem.logSuccessBlock(header,
				String.format("New block '%s':%d  is created on top of '%s'. ",
						opBlock.getFullHash(), opBlock.getBlockId(), opBlock.getStringValue(OpBlock.F_PREV_BLOCK_HASH) ));
		pm.capture();
		return opBlock;
	}

	public synchronized boolean compact() {
		Metric m = mBlockSaveSuperBlock.start();
		OpBlockChain savedParent = dataManager.saveMainBlockchain(blockchain.getParent());
		if(blockchain.getParent() != savedParent) {
			blockchain.changeToEqualParent(savedParent);
		}
		m.capture();
		
		m = mBlockCompact.start();
		OpBlockChain newParent = dataManager.compact(0, blockchain.getParent(), true);
		if(newParent != blockchain.getParent()) {
			blockchain.changeToEqualParent(newParent);
		}
		m.capture();
		
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
				BlocksListResult replicateBlockHeaders = formatter.fromJson(
						readerFromUrl(getReplicateUrl() + "blocks?from=" + from),
								BlocksListResult.class);
				LinkedList<OpBlock> headersToReplicate = replicateBlockHeaders.blocks;
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
							dataManager.insertOperation(o);
						}
					}
					replicateOneBlock(fullBlock);
				}
				return true;
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
				logSystem.logError(null, ErrorType.MGMT_REPLICATION_IO_FAILED, "Failed to replicate from " + getReplicateUrl(), e);
			}
		}
		return false;
	}

	private OpBlock downloadBlock(OpBlock header) throws MalformedURLException, IOException {
		URL downloadByHash = new URL(getReplicateUrl() + "block-by-hash?hash=" + header.getRawHash());
		OpBlock res = formatter.fromJson(new InputStreamReader(downloadByHash.openStream()), OpBlock.class);
		if(res.getBlockId() == -1) {
			return null;
		}
		return res;
	}
	
	public synchronized boolean replicateOneBlock(OpBlock block) {
		Metric m = mBlockSync.start();
		OpBlockChain blc = new OpBlockChain(blockchain.getParent(), blockchain.getRules());
		OpBlock res;
		DeletedObjectCtx hctx = new DeletedObjectCtx();
		res = blc.replicateBlock(block, hctx);
		m.capture();
		if(res == null) {
			return false;
		}
		res = replicateValidBlock(blc, res, hctx);
		if(res == null) {
			return false;
		}
		return true;
	}
	
	public synchronized Set<String> removeQueueOperations(Set<String> operationsToDelete) {
		Set<String> deleted = new TreeSet<String>();
		// handle non last operations - slow method
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
		dataManager.removeOperations(deleted);
		blockchain = blc;
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
	
	
	
	public String getCurrentStateDescription() {
		return statusDescription;
	}
	
	public String getCurrentState() {
		if(blockchain == null) {
			return "INITIALIZING";
		}
		if(blockchain.getStatus() == OpBlockChain.UNLOCKED) {
			return "READY";
		} else if(blockchain.getStatus() == OpBlockChain.LOCKED_STATE) {
			return "LOCKED";
		} else if(blockchain.getStatus() == OpBlockChain.LOCKED_BY_USER) {
			return "LOCKED_BY_USER";
		} else if(blockchain.getStatus() == OpBlockChain.LOCKED_OP_IN_PROGRESS) {
			return "OP_IN_PROGRESS";
		} else if(blockchain.getStatus() == OpBlockChain.LOCKED_ERROR) {
			return "ERROR (restart required)";
		}
		return "UNKNOWN";
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

	public Collection<OpIndexColumn> getIndicesForType(String type) {
		return dataManager.getIndicesForType(type);
	}
	
	public OpIndexColumn getIndex(String type, String indexId) {
		return dataManager.getIndex(type, indexId);
	}
	
	public void setBootstrapList(List<String> bootstrapList) {
		this.bootstrapList = bootstrapList;
	}
	
	public double getQueueCapacity() {
		int opsSize = 0;
		int opsCnt = 0;
		Deque<OpOperation> ops = blockchain.getQueueOperations();
		for (OpOperation o : ops) {
			opsCnt++;
			opsSize += formatter.opToJson(o).length();
		}
		return capacity(opsSize, opsCnt);
	}

	private double capacity(int size, int opsCnt) {
		double c1 = size / ((double) OpBlockchainRules.MAX_ALL_OP_SIZE_MB);
		double c2 = opsCnt / ((double) OpBlockchainRules.MAX_BLOCK_SIZE_OPS);
		return Math.max(c1, c2);
	}

	private List<OpOperation> pickupOpsFromQueue(double minCapacity, Collection<OpOperation> q) {
		int size = 0;
		int opsCnt = 0;
		List<OpOperation> candidates = new ArrayList<OpOperation>();
		for (OpOperation o : q) {
			int l = formatter.opToJson(o).length();
			if (size + l > OpBlockchainRules.MAX_ALL_OP_SIZE_MB) {
				break;
			}
			if (candidates.size() + 1 >= OpBlockchainRules.MAX_BLOCK_SIZE_OPS) {
				break;
			}
			size += l;
			opsCnt++;
			candidates.add(o);
		}
		if(capacity(size, opsCnt) >= minCapacity) {
			return candidates;
		}
		return null;
	}

	public Map<String, Map<String, OpIndexColumn>> getIndices() {
		return dataManager.getIndices();
	}

	public static class BlocksListResult {
		public LinkedList<OpBlock> blocks = new LinkedList<OpBlock>();
		public int blockDepth;
	}

	private static final PerformanceMetric mBlockAddOpp = PerformanceMetrics.i().getMetric("block.mgmt.addop");
	private static final PerformanceMetric mBlockCreate = PerformanceMetrics.i().getMetric("block.mgmt.create.total");
	private static final PerformanceMetric mBlockCreateAddOps = PerformanceMetrics.i().getMetric("block.mgmt.create.addops");
	private static final PerformanceMetric mBlockCreateValidate = PerformanceMetrics.i().getMetric("block.mgmt.create.validate");
	private static final PerformanceMetric mBlockCreateExtResources = PerformanceMetrics.i().getMetric("block.mgmt.create.extresources");
	private static final PerformanceMetric mBlockSync = PerformanceMetrics.i().getMetric("block.mgmt.sync");
	private static final PerformanceMetric mBlockReplicate = PerformanceMetrics.i().getMetric("block.mgmt.replicate.total");
	private static final PerformanceMetric mBlockSaveBlock = PerformanceMetrics.i().getMetric("block.mgmt.replicate.db.saveblock");
	private static final PerformanceMetric mBlockSaveHistory = PerformanceMetrics.i().getMetric("block.mgmt.replicate.db.savesuperblock");
	private static final PerformanceMetric mBlockSaveSuperBlock = PerformanceMetrics.i().getMetric("block.mgmt.replicate.db.savehistory");
	private static final PerformanceMetric mBlockCompact = PerformanceMetrics.i().getMetric("block.mgmt.replicate.compact");
	private static final PerformanceMetric mBlockRebase = PerformanceMetrics.i().getMetric("block.mgmt.replicate.rebase");

}
