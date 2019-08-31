package org.openplacereviews.opendb.ops;

import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;
import org.openplacereviews.opendb.ops.OpPrivateObjectInstancesById.CacheObject;
import org.openplacereviews.opendb.ops.PerformanceMetrics.Metric;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.DBConsensusManager.DBStaleException;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import java.security.KeyPair;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Stream;

import static org.openplacereviews.opendb.ops.OpBlock.*;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_VOTE;
import static org.openplacereviews.opendb.ops.OpObject.F_FINAL;
import static org.openplacereviews.opendb.ops.OpObject.F_OP;
import static org.openplacereviews.opendb.ops.OpObject.F_STATE;
import static org.openplacereviews.opendb.ops.OpObject.F_SUBMITTED_OP_HASH;
import static org.openplacereviews.opendb.ops.OpObject.F_VOTE;
import static org.openplacereviews.opendb.ops.OpOperation.F_REF;

/**
 *  Guidelines of object methods:
 *  1. This object doesn't expose any of the internal representation i.e. doesn't expose internal arrays or anything for modification
 *  In case information is returned by get methods it should be considered immutable.
 *  Now not all objects are protected by immutability OpBlock, OpOperations, OpObjects
 *  2. Atomic internal methods should never fail, in case atomic method fails the object moves to the state LOCKED_ERROR which can't be reverted
 *  and object needs to be recreated
 *  3. All change methods are synchronized in order to :
 *  	- maintain proper locked and locked error state.
 *      - to properly prepare for modification and know that none of the internal objects will change during validation & preparation
 *      - method compact / merge requires 2 objects to go synchronized
 *  4. Change methods return true/false and throw exception, if object remains unlocked it means that exception wasn't fatal
 *  5. There are 4 key atomic operations:
 *      - atomicAddOperationAfterPrepare  / addOperation - used to add operation into a queue
 *      - atomicCreateBlockFromAllOps / createBlock - combines queue operations into a block
 *      - atomicRebaseOperations / atomicRemoveOperation / rebaseOperations -
 *      		rebase operations to new parent, used to change parent for blockchain queue and delete ops from queue already present in new parent
 *      - atomicSetParent / changeToEqualParent - simple set new parent object which represents exactly same chain as previous parent, used to compact chain
 *
 */
public class OpBlockChain {

	public static final String OP_CHANGE_DELETE = "delete";
	public static final String OP_CHANGE_INCREMENT = "increment";
	public static final String OP_CHANGE_APPEND = "append";
	public static final String OP_CHANGE_SET = "set";
	
	
	public static final int LOCKED_ERROR = -1; // means it is locked and there was unrecoverable error during atomic operation
	public static final int UNLOCKED =  0; // unlocked and ready for operations
	public static final int LOCKED_OP_IN_PROGRESS = 1; // operation on blockchain is in progress and it will be unlocked after
	public static final int LOCKED_STATE = 2; // FINAL STATE. locked successfully and could be used as parent superblock
	public static final int LOCKED_BY_USER = 4; // locked by user and it could be unlocked by user
	public static final OpBlockChain NULL = new OpBlockChain(true);
	
	// 0-0 represents locked or unlocked state for blockchain
	private volatile int locked = UNLOCKED;
	// 0-1 nullable object is always root (in order to perform operations in sync)
	private final boolean nullObject;
	// 0-2 immutable blockchain rules to validate operations
	private final OpBlockchainRules rules;
	// 0-3 db access if it exists
	private final BlockDbAccessInterface dbAccess;

	// 1. parent chain
	private volatile OpBlockChain parent;

	// These objects should be stored on disk (DB)
	// 2. list of blocks, block hash ids link to blocks
	private final OpPrivateBlocksList blocks;

	// 3. stores information about last object by name in this blockchain
	private final Map<String, OpPrivateObjectInstancesById> objByName = new ConcurrentHashMap<>();

	// 4. operations to be stored like a queue
	private final Deque<OpOperation> queueOperations = new ConcurrentLinkedDeque<OpOperation>();

	private final Map<String, OpOperation> blockOperations = new ConcurrentHashMap<>();
	

	private OpBlockChain(boolean nullParent) {
		this.nullObject = true;
		this.rules = null;
		locked = LOCKED_STATE;
		this.dbAccess = null;
		this.blocks = new OpPrivateBlocksList();
	}

	public OpBlockChain(OpBlockChain parent, OpBlockchainRules rules) {
		if(parent == null) {
			throw new IllegalStateException("Parent can not be null, use null object for reference");
		}
		this.rules = rules;
		this.nullObject = false;
		this.dbAccess = null;
		this.blocks = new OpPrivateBlocksList();
		atomicSetParent(parent);
	}

	public OpBlockChain(OpBlockChain parent, Collection<OpBlock> headers, BlockDbAccessInterface dbAccess, OpBlockchainRules rules) {
		if(parent == null) {
			throw new IllegalStateException("Parent can not be null, use null object for reference");
		}
		if(dbAccess == null) {
			throw new IllegalStateException("This constructor for db access superblocks");
		}
		this.rules = rules;
		this.nullObject = false;
		this.dbAccess = dbAccess;
		this.blocks = new OpPrivateBlocksList(headers, parent.getSuperblocksDepth() + 1, this.dbAccess);
		atomicSetParent(parent);
	}

	public OpBlockChain(OpBlockChain copy, OpBlockChain parentToMerge, OpBlockchainRules rules) {
		this.rules = rules;
		this.nullObject = false;
		this.blocks = new OpPrivateBlocksList();
		this.dbAccess = null;
		if(parentToMerge == null || parentToMerge.isNullBlock() || copy.parent != parentToMerge) {
			throw new IllegalStateException("Wrong parameters to create object with merged parents");
		}
		copy.validateLocked();
		parentToMerge.validateLocked();

		atomicSetParent(parentToMerge.parent);
		copyAndMergeWithParent(copy, parentToMerge);
	}

	public synchronized void validateLocked() {
		if(nullObject) {
			return;
		}
		if(this.locked == UNLOCKED) {
			this.locked = LOCKED_STATE;
		} else if(this.locked != LOCKED_STATE) {
			throw new IllegalStateException("This chain is locked not by user or in a broken state");
		}
	}

	public boolean isQueueEmpty() {
		return queueOperations.isEmpty();
	}

	private void validateIsUnlocked() {
		if(nullObject) {
			throw new IllegalStateException("This chain is immutable (null chain)");
		}
		if(locked != UNLOCKED) {
			throw new IllegalStateException("This chain is immutable");
		}
	}

	public boolean isDbAccessed() {
		return dbAccess != null;
	}

	public int getStatus() {
		return locked;
	}

	public synchronized void lockByUser() {
		if(nullObject) {
			return;
		}
		if(this.locked == UNLOCKED) {
			this.locked = LOCKED_BY_USER;
		} else if(this.locked != LOCKED_BY_USER) {
			throw new IllegalStateException("This chain is locked not by user or in a broken state");
		}
	}


	public synchronized void unlockByUser() {
		if(nullObject) {
			return;
		}
		if(this.locked == LOCKED_BY_USER) {
			this.locked = UNLOCKED;
		} else if(this.locked != UNLOCKED) {
			throw new IllegalStateException("This chain is locked not by user or in a broken state");
		}
	}

	public synchronized OpBlock createBlock(String user, KeyPair keyPair) throws FailedVerificationException {
		OpBlock block = rules.createAndSignBlock(getQueueOperations(), getLastBlockHeader(), user, keyPair);
		validateIsUnlocked();
		boolean valid = rules.validateBlock(this, block, getLastBlockHeader(), true);
		if(!valid) {
			return null;
		}
		locked = LOCKED_OP_IN_PROGRESS;
		try {
			atomicCreateBlockFromAllOps(block);
			locked = UNLOCKED;
		} finally {
			if(locked == LOCKED_OP_IN_PROGRESS) {
				locked = LOCKED_ERROR;
			}
		}
		return block;
	}

	public synchronized boolean removeAllQueueOperations() {
		validateIsUnlocked();
		if(blocks.size() != 0) {
			return false;
		}
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		locked = LOCKED_OP_IN_PROGRESS;
		try {
			objByName.clear();
			queueOperations.clear();
			locked = UNLOCKED;
		} finally {
			if (locked == LOCKED_OP_IN_PROGRESS) {
				locked = LOCKED_ERROR;
			}
		}

		return true;
	}


	public synchronized OpBlock replicateBlock(OpBlock block) {
		return replicateBlock(block, null);
	}

	public synchronized OpBlock replicateBlock(OpBlock block, DeletedObjectCtx hctx) {
		block.checkImmutable();
		validateIsUnlocked();
		if (!isQueueEmpty()) {
			// can't replicate blocks when operations are not empty
			return null;
		}
		boolean valid = rules.validateBlock(this,
				block, getLastBlockHeader(), block.getBlockId() != 0);
		if (!valid) {
			return null;
		}
		locked = LOCKED_OP_IN_PROGRESS;
		try {
			for (OpOperation o : block.getOperations()) {
				LocalValidationCtx validationCtx = new LocalValidationCtx(block.getFullHash());
				validateAndPrepareOperation(o, validationCtx, hctx);
				atomicAddOperationAfterPrepare(o, validationCtx);
			}
			atomicCreateBlockFromAllOps(block);
			locked = UNLOCKED;
		} finally {
			if (locked == LOCKED_OP_IN_PROGRESS) {
				locked = LOCKED_ERROR;
			}
		}
		return block;
	}

	public synchronized boolean rebaseOperations(OpBlockChain newParent) {
		validateIsUnlocked();
		newParent.validateLocked();
		if(!newParent.isQueueEmpty()) {
			return false;
		}
		// calculate blocks and ops to be removed, all blocks must be present in new parent
		// if(blocks.size() > 0) { return false; }
		for(OpBlock bl : blocks.getAllBlockHeaders()) {
			int blDept = newParent.getBlockDepth(bl);
			if(blDept < 0) {
				return false;
			}
		}

		OpBlock lb = parent.getLastBlockHeader();
		if(lb != null && newParent.getBlockDepth(lb) == -1) {
			// rebase is not allowed
			return false;
		}
		locked = LOCKED_OP_IN_PROGRESS;
		try {
			atomicRebaseOperations(newParent);
			locked = UNLOCKED;
		} finally {
			if(locked == LOCKED_OP_IN_PROGRESS) {
				locked = LOCKED_ERROR;
			}
		}
		return true;
	}

	public synchronized boolean changeToEqualParent(OpBlockChain newParent) {
		if(nullObject) {
			return false;
		}
		newParent.validateLocked();
		if(!OUtils.equals(newParent.getLastBlockFullHash(), parent.getLastBlockFullHash())) {
			return false;
		}
		// operation doesn't require locking mechanism
		int status = locked;
		if(status != LOCKED_STATE && status != UNLOCKED) {
			return false;
		}
		locked = LOCKED_STATE;
		try {
			atomicSetParent(newParent);
			locked = status;
		} catch(RuntimeException e) {
			// it could be lost in between state cause, getStatus method is not recursive
			locked = LOCKED_ERROR;
			throw e;
		}
		return true;

	}

	public boolean addOperation(OpOperation op, DeletedObjectCtx historyObjectCtx) {
		return addOperation(op, false, historyObjectCtx);
	}

	public boolean addOperation(OpOperation op) {
		return addOperation(op, false, null);
	}

	public boolean validateOperation(OpOperation op) {
		return addOperation(op, true, null);
	}
	/**
	 * Adds operation and validates it to block chain
	 */
	private synchronized boolean addOperation(OpOperation op, boolean onlyValidate, DeletedObjectCtx historyObjectCtx) {
		op.checkImmutable();
		validateIsUnlocked();
		LocalValidationCtx validationCtx = new LocalValidationCtx("");
		boolean valid = validateAndPrepareOperation(op, validationCtx, historyObjectCtx);
		if(!valid || onlyValidate) {
			return valid;
		}
		locked = LOCKED_OP_IN_PROGRESS;
		try {
			atomicAddOperationAfterPrepare(op, validationCtx);
			locked = UNLOCKED;
		} finally {
			if(locked == LOCKED_OP_IN_PROGRESS) {
				locked = LOCKED_ERROR;
			}
		}
		return valid;
	}

	private void atomicAddOperationAfterPrepare(OpOperation u, LocalValidationCtx validationCtx) {
		List<List<String>> deletedRefs = u.getDeleted();
		String objType = u.getType();
		for (List<String> deletedRef : deletedRefs) {
			OpPrivateObjectInstancesById oinf = getOrCreateObjectsByIdMap(objType);
			OpObject dl = new OpObject(true);
			dl.setParentOp(u.getType(), u.getRawHash());
			oinf.add(deletedRef, dl);
		}
		queueOperations.add(u);
		for (OpObject editedOpOpbject : validationCtx.newObjsCache.keySet()) {
			OpPrivateObjectInstancesById oinf = getOrCreateObjectsByIdMap(objType);
			oinf.add(editedOpOpbject.getId(), editedOpOpbject);
		}
		if (u.getRef() != null && u.getRef().get(F_VOTE) != null) {
			OpObject voteObj = validationCtx.refObjsCache.get(F_VOTE);
			if (voteObj != null && voteObj.getStringValue(F_SUBMITTED_OP_HASH).equals(u.getRawHash())) {
				OpPrivateObjectInstancesById oinf = getOrCreateObjectsByIdMap(OP_VOTE);
				oinf.add(voteObj.getId(), voteObj);
			}
		}
	}

	private void atomicSetParent(OpBlockChain parent) {
		if (!parent.isNullBlock()) {
			if (this.rules != parent.rules) {
				throw new IllegalStateException("Blockchain rules should be consistent trhough whole chain");
			}
		}
		parent.validateLocked();
		if (!parent.isQueueEmpty()) {
			throw new IllegalStateException("Parent chain doesn't allow to have operations");
		}
		this.parent = parent;
	}

	private void atomicCreateBlockFromAllOps(OpBlock block) {
		if (dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		for (OpOperation o : queueOperations) {
			blockOperations.put(o.getRawHash(), o);
		}
		queueOperations.clear();
		blocks.addBlock(block, getSuperblocksDepth());

	}

	private void atomicRebaseOperations(OpBlockChain newParent) {
		// all blocks must be present in new parent
		List<OpOperation> ops = new ArrayList<>(queueOperations);
		blocks.clear();
		blockOperations.clear();
		queueOperations.clear();
		objByName.clear();
		Iterator<OpOperation> it = ops.iterator();
		while(it.hasNext()) {
			OpOperation o = it.next();
			if(newParent.getOperationByHash(o.getRawHash()) != null) {
				it.remove();
			}
		}
		for (OpOperation o : ops) {
			LocalValidationCtx validationCtx = new LocalValidationCtx("<queue>");
			validateAndPrepareOperation(o, validationCtx, null);
			atomicAddOperationAfterPrepare(o, validationCtx);
		}
		atomicSetParent(newParent);
	}


	private void copyAndMergeWithParent(OpBlockChain copy, OpBlockChain parent ) {
		if (copy.isDbAccessed() || parent.isDbAccessed()) {
			throw new UnsupportedOperationException();
		}
		// 1. add blocks and their hashes
		blocks.copyAndMerge(copy.blocks, parent.blocks, parent.getSuperblocksDepth());

		// 2. merge named objects
		TreeSet<String> types = new TreeSet<String>(parent.objByName.keySet());
		types.addAll(copy.objByName.keySet());
		for (String type : types){
			OpPrivateObjectInstancesById nid = getOrCreateObjectsByIdMap(type);
			OpPrivateObjectInstancesById cid = copy.objByName.get(type);
			OpPrivateObjectInstancesById pid = parent.objByName.get(type);
			nid.putObjects(pid, true);
			nid.putObjects(cid, true);

		}
	}

	public Deque<OpOperation> getQueueOperations() {
		return queueOperations;
	}


	public OpBlockChain getParent() {
		return parent;
	}

	public OpBlockchainRules getRules() {
		return rules;
	}

	public OpBlock getLastBlockHeader() {
		if (nullObject) {
			return null;
		}
		OpBlock h = blocks.getLastBlockHeader();
		if (h == null) {
			return parent.getLastBlockHeader();
		}
		return h;
	}

	public String getLastBlockFullHash() {
		OpBlock b = getLastBlockHeader();
		return b == null ? "" : b.getFullHash();
	}

	public String getLastBlockRawHash() {
		OpBlock b = getLastBlockHeader();
		return b == null ? "" : b.getRawHash();
	}

	public int getLastBlockId() {
		OpBlock o = getLastBlockHeader();
		return o != null ? o.getBlockId() : -1;
	}

	public int getDepth() {
		return getLastBlockId() + 1;
	}


	public boolean isNullBlock() {
		return nullObject;
	}

	public int getSuperblocksDepth() {
		if(nullObject) {
			return 0;
		}
		return parent.getSuperblocksDepth() + 1;
	}

	public Deque<OpBlock> getSuperblockHeaders() {
		return blocks.getAllBlockHeaders();
	}

	public Deque<OpBlock> getSuperblockFullBlocks() {
		return blocks.getAllBlocks();
	}


	public Map<String, Map<CompoundKey, OpObject>> getRawSuperblockObjects() {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		Map<String, Map<CompoundKey, OpObject>> mp = new TreeMap<String, Map<CompoundKey, OpObject>>();
		for(String type : objByName.keySet()) {
			OpPrivateObjectInstancesById bid = objByName.get(type);
			mp.put(type, bid.getRawObjects());
		}
		return mp;
	}

	public List<OpBlock> getBlockHeaders(int depth) {
		List<OpBlock> lst = new ArrayList<>();
		fetchBlockHeaders(lst, depth);
		return lst;
	}

	public OpBlock getBlockHeadersById(int id) {
		if(nullObject) {
			return null;
		}
		if(parent.getLastBlockId() >= id) {
			return parent.getBlockHeadersById(id);
		}
		for(OpBlock o : blocks.getAllBlockHeaders()) {
			if(o.getBlockId() == id) {
				return o;
			}
		}
		return null;
	}

	public OpBlock getFullBlockByBlockId(int id) {
		OpBlock b = getBlockHeadersById(id);
		if (b != null) {
			return getFullBlockByRawHash(b.getRawHash());
		}

		return null;
	}

	public OpBlock getGeneratedOpBlockWithOperationsByObjectId(List<String> id) {
		OpBlock opBlock = new OpBlock();
		if (id.size() > 1) {
			String type = id.get(0);
			id = id.subList(1, id.size());
			List<OpObject> opObjectList = new ArrayList<>();
			getAllObjectsByName(type, id, opObjectList);
			for (OpObject opObject : opObjectList) {
				OpOperation opOperation = getOperationByHash(opObject.parentHash);
				if (opOperation != null) {
					opBlock.addOperation(opOperation);
				}
			}
		} else {

		}

		return opBlock;
	}

	public String getSuperBlockHash() {
		return blocks.getSuperBlockHash();
	}

	public int getSuperblockSize() {
		return blocks.size();
	}


	public int getBlockDepth(OpBlock block) {
		if(nullObject) {
			return -1;
		}
		OpBlock n = blocks.getBlockHeaderByHash(block.getRawHash());
		if(n != null) {
			return n.getBlockId();
		}
		return parent.getBlockDepth(block);
	}


	public OpBlock getBlockHeaderByRawHash(String hash) {
		if(nullObject) {
			return null;
		}
		OpBlock n = blocks.getBlockHeaderByHash(hash);
		if(n != null) {
			return n;
		}
		return parent.getBlockHeaderByRawHash(hash);
	}

	public OpBlock getFullBlockByRawHash(String hash) throws DBStaleException {
		if(nullObject) {
			return null;
		}
		OpBlock n = blocks.getFullBlockByHash(hash);
		if(n != null) {
			return n;
		}
		return parent.getFullBlockByRawHash(hash);
	}

	public OpOperation getOperationByHash(String rawHash) throws DBStaleException {
		if(nullObject) {
			return null;
		}
		OpOperation o;
		if(dbAccess != null) {
			o = dbAccess.getOperation(rawHash);
		} else {
			o = blockOperations.get(rawHash);
			if(o == null) {
				for(OpOperation ops : queueOperations) {
					if(ops.getRawHash().equals(rawHash)) {
						return ops;
					}
				}
			}
		}
		if(o != null) {
			return o;
		}
		return parent.getOperationByHash(rawHash);
	}

	public OpObject getObjectByName(String type, String key) throws DBStaleException {
		return getObjectByName(type, key, null);
	}

	public OpObject getObjectByName(String type, String key, String secondary) throws DBStaleException {
		if (isNullBlock()) {
			return null;
		}
		OpPrivateObjectInstancesById ot = getOrCreateObjectsByIdMap(type);
		if (ot != null) {
			Metric m = mFetchById.start();
			OpObject obj = ot.getObjectById(key, secondary);
			m.capture();
			if (obj != null) {
				if(obj.isDeleted()) {
					return null;
				}
				return obj;
			}
		}
		return parent.getObjectByName(type, key, secondary);
	}

	public OpObject getObjectByName(String type, List<String> o) throws DBStaleException {
		if (isNullBlock()) {
			return null;
		}
		OpPrivateObjectInstancesById ot = getOrCreateObjectsByIdMap(type);
		if (ot != null) {
			Metric m = mFetchById.start();
			OpObject obj = ot.getObjectById(o);
			m.capture();
			if (obj != null) {
				if(obj.isDeleted()) {
					return null;
				}
				return obj;
			}
		}
		return parent.getObjectByName(type, o);
	}

	public OpObject getAllObjectsByName(String type, List<String> o, List<OpObject> opObjects) throws DBStaleException {
		if (isNullBlock()) {
			return null;
		}
		OpPrivateObjectInstancesById ot = getOrCreateObjectsByIdMap(type);
		if (ot != null) {
			Metric m = mFetchById.start();
			OpObject obj = ot.getObjectById(o);
			m.capture();
			if (obj != null) {
				opObjects.add(obj);
			}
		}
		return parent.getAllObjectsByName(type, o, opObjects);
	}

	public void setCacheAfterSearch(ObjectsSearchRequest request, Object cacheObject) {
		if(request.objToSetCache != null) {
			request.objToSetCache.setCacheObject(cacheObject, request.editVersion);
		}
	}
	
	public int countAllObjects(String type) {
		if(isNullBlock()) {
			return 0;
		}
		OpPrivateObjectInstancesById oi = getOrCreateObjectsByIdMap(type);
		int sz = oi.countObjects();
		return sz + parent.countAllObjects(type);
	}

	public void fetchAllObjects(String type, ObjectsSearchRequest request) throws DBStaleException {
		if(isNullBlock()) {
			return;
		}
		Metric m = PerformanceMetrics.i().getMetric("blc.fetch.all.total").start();
		OpPrivateObjectInstancesById oi = getOrCreateObjectsByIdMap(type);
		if(oi == null) {
			parent.fetchAllObjects(type, request);
		} else {
			request.editVersion = oi.getEditVersion();
			request.objToSetCache = oi;
			if (request.requestCache) {
				CacheObject co = oi.getCacheObject();
				if (co != null && co.cacheVersion == request.editVersion) {
					request.cacheObject = co.cacheObject;
					request.cacheVersion = co.cacheVersion;
					return;
				}
			}
			Map<CompoundKey, OpObject> res = fetchObjectsInternal(type, request, null);
			request.setResult(res);
		}
		m.capture();
	}
	
	
	public void fetchObjectsByIndex(String type, OpIndexColumn index, ObjectsSearchRequest request, Object... argsToSearch) throws DBStaleException {
		Metric m = PerformanceMetrics.i().getMetric("blc.fetch." + index.getIndexId() + ".total").start();
		Map<CompoundKey, OpObject> res = fetchObjectsInternal(type, request, index, argsToSearch);
		request.setResult(res);
		m.capture();
	}
	

	private Map<CompoundKey, OpObject> fetchObjectsInternal(String type, ObjectsSearchRequest request, OpIndexColumn col, Object... args) throws DBStaleException {
		Map<CompoundKey, OpObject> res = new LinkedHashMap<>();
		if(isNullBlock()) {
			return res;
		}
		String mid = "blc.fetch." + (col == null ? "all" : col.getIndexId());
		mid += isDbAccessed()?  ".db" : ".ram";
		Metric m = PerformanceMetrics.i().getMetric(mid).start();
		OpPrivateObjectInstancesById o = getOrCreateObjectsByIdMap(type);
		// don't check for all queries
		if (o != null) {
			Stream<Entry<CompoundKey, OpObject>> stream = o.fetchObjects(request, getSuperblockSize(), col, args);
			Iterator<Entry<CompoundKey, OpObject>> it = stream.iterator();
			while (it.hasNext()) {
				Entry<CompoundKey, OpObject> e = it.next();
				res.put(e.getKey(), e.getValue());
				request.internalProgress++;
				if (request.limit >= 0 && request.internalProgress >= request.limit) {
					m.capture();
					return res;
				}
			}
		}
		m.capture();
		// capture parent results
		Map<CompoundKey, OpObject> prres = parent.fetchObjectsInternal(type, request, col, args);
		// HERE we need to check that newer version doesn't exist in current blockchain
		prres.putAll(res);
		if (col != null) {
			for (CompoundKey c : prres.keySet()) {
				OpObject i = o.getByKey(c);
				if (i != null && !res.containsKey(c)) {
					// object was overridden
					prres.remove(c);
				}
			}
		}
		res = prres;
		return res;
	}

	private OpPrivateObjectInstancesById getOrCreateObjectsByIdMap(String type) {
		// create is allowed only when status is not locked
		if (nullObject) {
			return null;
		}
		OpPrivateObjectInstancesById oi = objByName.get(type);
		if(oi == null) {
			oi = new OpPrivateObjectInstancesById(type, dbAccess);
			objByName.put(type, oi);
		}
		return oi;
	}

	static int getIndexFromAbsRef(String r) {
		int i = r.indexOf(':');
		if (i == -1) {
			return 0;
		}
		return Integer.parseInt(r.substring(i + 1));
	}

	static String getHashFromAbsRef(String r) {
		int i = r.indexOf(':');
		if(i == -1) {
			return r;
		}
		return r.substring(0, i);
	}

	private void fetchBlockHeaders(List<OpBlock> lst, int depth) {
		if(nullObject) {
			return;
		}
		Collection<OpBlock> blockHeaders = blocks.getAllBlockHeaders();
		lst.addAll(blockHeaders);
		if(depth != -1) {
			depth -= blockHeaders.size();
			if(depth < 0) {
				return;
			}
		}
		parent.fetchBlockHeaders(lst, depth);
	}

	private boolean validateAndPrepareOperation(OpOperation u, LocalValidationCtx ctx, DeletedObjectCtx hctx) {
		Metric pm = mPrepareTotal.start();
		if(OUtils.isEmpty(u.getRawHash())) {
			return rules.error(u, ErrorType.OP_HASH_IS_NOT_CORRECT, u.getHash(), "");
		}
		OpOperation oin = getOperationByHash(u.getRawHash());
		if(oin != null) {
			return rules.error(u, ErrorType.OP_HASH_IS_DUPLICATED, u.getHash(), ctx.blockHash);
		}
		u.updateObjectsRef();
		boolean valid = true;
		ctx.ids.clear();
		Metric m = mPrepareDelete.start();
		valid = prepareDeletedObjects(u, ctx, hctx);
		m.capture();
		if(!valid) {
			return false;
		}
		// should be called after prepareDeletedObjects (so cache is prepared)
		m = mPrepareCreate.start();
		valid = prepareCreatedObjects(u, ctx);
		m.capture();
		if(!valid) {
			return false;
		}
		m = mPrepareEdit.start();
		valid = prepareEditedObjects(u, ctx);
		m.capture();
		if (!valid) {
			return false;
		}
		m = mPrepareRef.start();
		valid = prepareReferencedObjects(u, ctx);
		if(!valid) {
			return valid;
		}
		m.capture();
		pm.capture();
		valid = rules.validateOp(this, u, ctx);
		if(!valid) {
			return valid;
		}
		if(u.getCacheObject(OpObject.F_TIMESTAMP_ADDED) == null) {
			u.putCacheObject(OpObject.F_TIMESTAMP_ADDED, System.currentTimeMillis());
		}
		return true;
	}

	private boolean prepareReferencedObjects(OpOperation u, LocalValidationCtx ctx) {
		Map<String, List<String>> refs = u.getRef();
		if (refs != null) {
			Iterator<Entry<String, List<String>>> it = refs.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, List<String>> e = it.next();
				String refName = e.getKey();
				List<String> refObjName = e.getValue();
				OpObject oi = null;
				if (refObjName.size() > 1) {
					// type is necessary
					OpBlockChain blc = this;
					String objType = refObjName.get(0);
					List<String> refKey = refObjName.subList(1, refObjName.size());
					while (blc != null && oi == null) {
						oi = blc.getObjectByName(objType, refKey);
						blc = blc.parent;
					}
				}
				if (oi == null) {
					return rules.error(u, ErrorType.REF_OBJ_NOT_FOUND, u.getHash(), refObjName);
				}
				ctx.refObjsCache.put(refName, oi);
			}

			OpObject voteObject = ctx.refObjsCache.get(F_VOTE);
			if (voteObject != null) {
				if (OP_VOTE.equals(voteObject.getParentType())) {
					if (F_FINAL.equals(voteObject.getStringValue(F_STATE))) {
						return rules.error(u, ErrorType.VOTE_VOTING_OBJ_IS_FINAL, u.getHash(), voteObject);
					}
					if (validateCurrentOpWithRefVoteOp(u, voteObject)) {
						OpObject newVoteObject = new OpObject(voteObject);
						newVoteObject.putStringValue(F_STATE, F_FINAL);
						newVoteObject.putStringValue(F_SUBMITTED_OP_HASH, u.getRawHash());
						ctx.refObjsCache.put(F_VOTE, newVoteObject);
					} else {
						return rules.error(u, ErrorType.VOTE_OP_IS_NOT_SAME, u.getHash(), u, voteObject.getStringObjMap(F_OP));
					}
				} else {
					return rules.error(u, ErrorType.VOTE_OP_SUPPORT_ONLY_SYS_VOTE_TYPE, u.getHash(), voteObject.getParentType());
				}
			}
		}

		if (getObjectByName(OpBlockchainRules.OP_OPERATION, u.getType()) != null) {
			ctx.refObjsCache.put("op", getObjectByName(OpBlockchainRules.OP_OPERATION, u.getType()));
		}
		return true;
	}

	private boolean validateCurrentOpWithRefVoteOp(OpOperation newOp, OpObject voteObject) {
		OpOperation cpyNewOp = removeOpFieldsForValidation(new OpOperation(newOp, false));
		OpOperation refOp = removeOpFieldsForValidation(rules.getFormatter().parseOperation(
				rules.getFormatter().fullObjectToJson(voteObject.getStringObjMap(F_OP))));

		return cpyNewOp.equals(refOp);
	}

	// remove ref, signature, hash and signedBy for new and ref.op operations
	private OpOperation removeOpFieldsForValidation(OpOperation op) {
		op.remove(F_REF);
		op.remove(F_SIGNATURE);
		op.remove(F_HASH);
		op.remove(F_SIGNED_BY);

		return op;
	}

	private boolean prepareDeletedObjects(OpOperation u, LocalValidationCtx ctx, DeletedObjectCtx hctx) {
		List<List<String>> deletedRefs = u.getDeleted();
		ctx.deletedObjsCache.clear();

		for(int i = 0; i < deletedRefs.size(); i++) {
			OpObject opObject = getObjectByName(u.getType(), deletedRefs.get(i));
			if(opObject == null) {
				return rules.error(u, ErrorType.DEL_OBJ_NOT_FOUND, u.getHash(), deletedRefs.get(i));
			}
			if(!ctx.ids.add(opObject.getId())) {
				return rules.error(u, ErrorType.OBJ_MODIFIED_TWICE_IN_SAME_OPERATION, u.getHash(),
						opObject.getId());
			}
			if (hctx != null) {
				hctx.putObjectToDeleteCache(u.getHash(), opObject);
			}
			ctx.deletedObjsCache.add(opObject);
		}
		return true;
	}

	private boolean prepareCreatedObjects(OpOperation u, LocalValidationCtx ctx) {
		List<OpObject> list = u.getCreated();
		if (list.size() > OpBlockchainRules.MAX_AMOUNT_CREATED_OBJ_FOR_OP) {
			return rules.error(u, ErrorType.LIMIT_OF_CREATED_OBJ_FOR_OP_WAS_EXCEEDED, u.getHash());
		}
		for(int i = 0; i < list.size(); i++) {
			OpObject newObject = list.get(i);
			if (newObject.getId().isEmpty()) {
				newObject = new OpObject(newObject);
				newObject.setId(u.getRawHash(), String.valueOf(i));
				newObject.makeImmutable();
			}
			if(!ctx.ids.add(newObject.getId())) {
				return rules.error(u, ErrorType.OBJ_MODIFIED_TWICE_IN_SAME_OPERATION, u.getHash(), newObject.getId());
			}
			ctx.newObjsCache.put(newObject, null);
		}
		return true;
	}
	
	@SuppressWarnings("unchecked")
	private boolean prepareEditedObjects(OpOperation u, LocalValidationCtx ctx) {
		List<OpObject> editedObjs = u.getEdited();
		for (OpObject editObject : editedObjs) {
			List<String> id = editObject.getId();
			// check duplicates in same operation
			if (!ctx.ids.add(id)) {
				return rules.error(u, ErrorType.OBJ_MODIFIED_TWICE_IN_SAME_OPERATION, u.getHash(), id);
			}
			OpObject currentObject = getObjectByName(u.getType(), id);
			if (currentObject == null) {
				return rules.error(u, ErrorType.EDIT_OBJ_NOT_FOUND, u.getHash(), id);
			}
			if (u.getType().equals(OP_VOTE)) {
				if (currentObject.getStringValue(F_STATE).equals(F_FINAL)) {
					return rules.error(u, ErrorType.VOTE_VOTING_OBJ_IS_FINAL, u.getHash(), currentObject.getId());
				}
			}
			OpObject newObject = new OpObject(currentObject);
			Map<String, Object> currentExpectedFields = editObject.getCurrentEditFields();
			if (currentExpectedFields != null) {
				Iterator<Entry<String, Object>> itEditCurrentFields = currentExpectedFields.entrySet().iterator();
				while (itEditCurrentFields.hasNext()) {
					Entry<String, Object> fieldsPair = itEditCurrentFields.next();
					Object expectedOldValF = currentObject.getFieldByExpr(fieldsPair.getKey());
					if (!OUtils.equals(fieldsPair.getValue(), expectedOldValF)) {
						return rules.error(u, ErrorType.EDIT_OLD_FIELD_VALUE_INCORRECT, u.getHash(),
								fieldsPair.getKey(), fieldsPair.getValue(), expectedOldValF);
					}
				}
			}
			Map<String, Object> changedMap = editObject.getChangedEditFields();
			for (Map.Entry<String, Object> e : changedMap.entrySet()) {
				// evaluate changes for new object
				String fieldExpr = e.getKey();
				Object op = e.getValue();
				String opId = op.toString();
				Object opValue = null;
				if (op instanceof Map) {
					Entry<String, Object> ee = ((Map<String, Object>) op).entrySet().iterator().next();
					opId = ee.getKey();
					opValue = ee.getValue();
				}
				try {
					boolean checkCurrentFieldSpecified = false;
					if (OP_CHANGE_DELETE.equals(opId)) {
						newObject.setFieldByExpr(fieldExpr, null);
						checkCurrentFieldSpecified = true;
					} else if (OP_CHANGE_SET.equals(opId)) {
						newObject.setFieldByExpr(fieldExpr, opValue);
						checkCurrentFieldSpecified = true;
					} else if (OP_CHANGE_APPEND.equals(opId)) {
						Object oldObject = newObject.getFieldByExpr(fieldExpr);
						if (oldObject == null) {
							List<Object> args = new ArrayList<>(1);
							args.add(opValue);
							newObject.setFieldByExpr(fieldExpr, args);
							checkCurrentFieldSpecified = false;
						} else if (oldObject instanceof List) {
							((List<Object>) oldObject).add(opValue);
							checkCurrentFieldSpecified = false;
						} else if (oldObject instanceof Map) {
							TreeMap<String, Object> value = (TreeMap<String, Object>) opValue;
							if (value != null) {
								((Map<String, Object>) oldObject).putAll(value);
								checkCurrentFieldSpecified = false;
							}
						} else {
							return rules.error(u, ErrorType.EDIT_OP_INCREMENT_ONLY_FOR_NUMBERS, fieldExpr, oldObject);
						}
					} else if (OP_CHANGE_INCREMENT.equals(opId)) {
						Object oldObject = newObject.getFieldByExpr(fieldExpr);
						if (oldObject == null) {
							newObject.setFieldByExpr(fieldExpr, 1);
							checkCurrentFieldSpecified = false;
						} else if (oldObject instanceof Number) {
							newObject.setFieldByExpr(fieldExpr, ((Number) oldObject).longValue() + 1);
							checkCurrentFieldSpecified = false;
						} else {
							return rules.error(u, ErrorType.EDIT_OP_INCREMENT_ONLY_FOR_NUMBERS, fieldExpr, oldObject);
						}
					} else {
						return rules.error(u, ErrorType.EDIT_CHANGE_DID_NOT_SPECIFY_CURRENT_VALUE, opId);
					}
					boolean currentNotSpecified = currentExpectedFields == null || 
							!currentExpectedFields.containsKey(fieldExpr);
					if (checkCurrentFieldSpecified && currentNotSpecified &&
							currentObject.getFieldByExpr(fieldExpr) != null) {
						return rules.error(u, ErrorType.EDIT_CHANGE_DID_NOT_SPECIFY_CURRENT_VALUE, u.getHash(), fieldExpr);
					}
				} catch(IndexOutOfBoundsException | IllegalArgumentException ex) {
					return rules.error(u, ErrorType.EDIT_OBJ_NOT_FOUND, u.getHash(), fieldExpr);
				}
			}
			newObject.parentHash = u.getRawHash();
			newObject.makeImmutable();
			ctx.newObjsCache.put(newObject, currentObject);
		}
		return true;
	}

	@Override
	public String toString() {
		return getSuperBlockHash();
	}

	// no multi thread issue (used only in synchronized blocks)
	public static class LocalValidationCtx {
		final String blockHash;
		Set<List<String>> ids = new HashSet<List<String>>();
		Map<String, OpObject> refObjsCache = new HashMap<String, OpObject>();
		List<OpObject> deletedObjsCache = new ArrayList<OpObject>();
		Map<OpObject, OpObject> newObjsCache = new HashMap<OpObject, OpObject>();

		public LocalValidationCtx(String bhash) {
			blockHash = bhash;
		}
	}

	public interface BlockDbAccessInterface {

		OpObject getObjectById(String type, CompoundKey k) throws DBStaleException ;

		/**
		 * extraParamsWithCondition[0] - extra and "sql condition"
		 * extraParamsWithCondition[1+...] - parameters to bind
		 */
		Stream<Map.Entry<CompoundKey, OpObject>> streamObjects(String type, int limit, boolean onlyKeys, Object... extraParamsWithCondition) throws DBStaleException;
		
		int countObjects(String type, Object... extraParamsWithCondition) throws DBStaleException;

		OpOperation getOperation(String rawHash) throws DBStaleException ;

		// Very memory consuming operation
		Deque<OpBlock> getAllBlocks(Collection<OpBlock> blockHeaders) throws DBStaleException ;

		OpBlock getBlockByHash(String rawHash) throws DBStaleException ;

	}
	
	public static class DeletedObjectCtx {
		public Map<String, List<OpObject>> deletedObjsCache = new LinkedHashMap<>();

		public void putObjectToDeleteCache(String key, OpObject opObject) {
			List<OpObject> list = deletedObjsCache.get(key);
			if (list == null) {
				list = new ArrayList<>();
			}
			list.add(opObject);
			deletedObjsCache.put(key, list);
		}
	}

	public static class ObjectsSearchRequest {
		public int editVersion;
		public int limit = -1;
		public boolean requestCache = false;
		public SearchType searchType = SearchType.EQUALS;
		public boolean requestOnlyKeys = false;

		public List<CompoundKey> keys = new ArrayList<CompoundKey>();
		public List<OpObject> result = new ArrayList<OpObject>();
		public int cacheVersion = -1;
		public Object cacheObject;

		OpPrivateObjectInstancesById objToSetCache;
		int internalProgress;
		
		public void setResult(Map<CompoundKey, OpObject> res) {
			Iterator<Entry<CompoundKey, OpObject>> it = res.entrySet().iterator();
			while(it.hasNext()) {
				Entry<CompoundKey, OpObject> e = it.next();
				if (e.getValue() != null && !e.getValue().isDeleted()) {
					if (!requestOnlyKeys) {
						result.add(e.getValue());
					}
					keys.add(e.getKey());
				}
			}
		}
	}
	
	public enum SearchType {
		EQUALS
	}
	
	private static final PerformanceMetric mPrepareCreate = PerformanceMetrics.i().getMetric("blc.prepare.create");
	private static final PerformanceMetric mPrepareEdit = PerformanceMetrics.i().getMetric("blc.prepare.edit");
	private static final PerformanceMetric mPrepareDelete = PerformanceMetrics.i().getMetric("blc.prepare.edit");
	private static final PerformanceMetric mPrepareRef = PerformanceMetrics.i().getMetric("blc.prepare.ref");
	private static final PerformanceMetric mPrepareTotal = PerformanceMetrics.i().getMetric("blc.prepare.total");
	
	private static final PerformanceMetric mFetchById = PerformanceMetrics.i().getMetric("blc.fetch.byid");


}