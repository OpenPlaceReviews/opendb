package org.openplacereviews.opendb.ops;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;
import org.openplacereviews.opendb.ops.OpPrivateObjectInstancesById.CacheObject;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.ops.de.OperationDeleteInfo;

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
	
	
	public static final int LOCKED_ERROR = -1; // means it is locked and there was unrecoverable error during atomic operation  
	public static final int UNLOCKED =  0; // unlocked and ready for operations
	public static final int LOCKED_OP_IN_PROGRESS = 1; // operation on blockchain is in progress and it will be unlocked after
	public static final int LOCKED_STATE = 2; // locked successfully and could be used as parent superblock
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
	private final OpPrivateBlocksList blocks ;
	
	// 3. stores operation list and information about created and deleted objects in this blockchain
	private final OpPrivateOperations operations ;
	
	// 4. stores information about last object by name in this blockchain
	private final Map<String, OpPrivateObjectInstancesById> objByName = new ConcurrentHashMap<>();
	
	
	
	private OpBlockChain(boolean nullParent) {
		this.nullObject = true;
		this.rules = null;
		locked = LOCKED_STATE;
		this.dbAccess = null;
		this.operations = new OpPrivateOperations(null);
		this.blocks = new OpPrivateBlocksList();
	}
	
	public OpBlockChain(OpBlockChain parent, OpBlockchainRules rules) {
		if(parent == null) {
			throw new IllegalStateException("Parent can not be null, use null object for reference");
		}
		this.rules = rules;
		this.nullObject = false;
		this.dbAccess = null;
		this.operations = new OpPrivateOperations(null);
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
		this.operations = new OpPrivateOperations(this.dbAccess);
		this.blocks = new OpPrivateBlocksList(headers, parent.getSuperblocksDepth() + 1, this.dbAccess);
		atomicSetParent(parent);
	}
	
	public OpBlockChain(OpBlockChain copy, OpBlockChain parentToMerge, OpBlockchainRules rules) {
		this.rules = rules;
		this.nullObject = false;
		this.operations = new OpPrivateOperations(null);
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
		OpBlock block = rules.createAndSignBlock(operations.getQueueOperations(), getLastBlockHeader(), user, keyPair);
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
		locked = LOCKED_OP_IN_PROGRESS;
		try {
			operations.clearQueueOperations(true);
			objByName.clear();
			locked = UNLOCKED;
		} finally {
			if (locked == LOCKED_OP_IN_PROGRESS) {
				locked = LOCKED_ERROR;
			}
		}
		return true;
	}
	
	public synchronized Set<String> removeQueueOperations(Set<String> operationsToDelete) {
		validateIsUnlocked();
		Iterator<OpOperation> descendingIterator = operations.getQueueOperations().descendingIterator();
		OpOperation nonDeletedLast = null;
		while(descendingIterator.hasNext()) {
			OpOperation no = descendingIterator.next();
			if(nonDeletedLast == null){ 
				if(!operationsToDelete.contains(no.getRawHash())) {
					nonDeletedLast = no;
				} 
			} else {
				if(operationsToDelete.contains(no.getRawHash())) {
					rules.error(nonDeletedLast, ErrorType.MGMT_CANT_DELETE_NON_LAST_OPERATIONS, nonDeletedLast.getRawHash(), no.getRawHash());
				}
			}
		}
		Set<String> result;
		locked = LOCKED_OP_IN_PROGRESS;
		try {
			result = atomicDeleteOperations(operationsToDelete);
			for(OpPrivateObjectInstancesById o : objByName.values()) {
				o.resetAfterEdit();
			}
			locked = UNLOCKED;
		} finally {
			if (locked == LOCKED_OP_IN_PROGRESS) {
				locked = LOCKED_ERROR;
			}
		}
		return result;
	}

	public synchronized OpBlock replicateBlock(OpBlock block) {
		block.checkImmutable();
		validateIsUnlocked();
		if (!operations.isQueueEmpty()) {
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
				validateAndPrepareOperation(o, validationCtx);
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
		if(!newParent.operations.isQueueEmpty()) {
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
	
	public boolean addOperation(OpOperation op) {
		return addOperation(op, false);
	}
	
	public boolean validateOperation(OpOperation op) {
		return addOperation(op, true);
	}
	/**
	 * Adds operation and validates it to block chain
	 */
	private synchronized boolean addOperation(OpOperation op, boolean onlyValidate) {
		op.checkImmutable();
		validateIsUnlocked();
		LocalValidationCtx validationCtx = new LocalValidationCtx("");
		boolean valid = validateAndPrepareOperation(op, validationCtx);
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
		List<String> deletedRefs = u.getOld();
		for(int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = getHashFromAbsRef(delRef);
			int delInd = getIndexFromAbsRef(delRef);
			OperationDeleteInfo oinfo = operations.addDeletedObject(delHash, delInd, u);
			for (OpObject delObj : oinfo.op.getNew()) {
				List<String> id = delObj.getId();
				if (id != null && id.size() > 0) {
					String objType = u.getType();
					OpPrivateObjectInstancesById oinf = getOrCreateObjectsByIdMap(objType);
					oinf.add(id, null);
				}
			}
			
		}
		operations.addNewOperation(u);
		for (OpObject newObj : u.getNew()) {
			List<String> id = newObj.getId();
			if (id != null && id.size() > 0) {
				String objType = u.getType();
				OpPrivateObjectInstancesById oinf = getOrCreateObjectsByIdMap(objType);
				oinf.add(id, newObj);
			}
		}
		
		
	}
	
	private void atomicSetParent(OpBlockChain parent) {
		if(!parent.isNullBlock()) {
			if(this.rules != parent.rules) {
				throw new IllegalStateException("Blockchain rules should be consistent trhough whole chain");
			}
		}
		parent.validateLocked();
		if(!parent.operations.isQueueEmpty()) {
			throw new IllegalStateException("Parent chain doesn't allow to have operations");
		}
		this.parent = parent;
	}

	private void atomicCreateBlockFromAllOps(OpBlock block) {
		operations.clearQueueOperations(false);
		blocks.addBlock(block, getSuperblocksDepth());
		
	}

	private void atomicRebaseOperations(OpBlockChain newParent) {
		// all blocks must be present in new parent
		for(OpBlock b : blocks.getAllBlocks()) {
			for(OpOperation o : b.getOperations()) {
				operations.removeOperationInfo(o);
				atomicRemoveOperationObj(o, null);
			}
		}
		blocks.clear();
		Set<String> operationsToDelete = new TreeSet<String>();
		for(OpOperation o : operations.getQueueOperations()) {
			OperationDeleteInfo odi = newParent.getOperationInfo(o.getRawHash());
			if(odi != null && odi.create) {
				operationsToDelete.add(o.getRawHash());
			}
		}
		atomicDeleteOperations(operationsToDelete);
		
		for(OpPrivateObjectInstancesById o : objByName.values()) {
			o.resetAfterEdit();
		}
		atomicSetParent(newParent);
	}

	private Set<String> atomicDeleteOperations(Set<String> operationsToDelete) {
		Set<String> deletedOps = new TreeSet<>();
		Map<String, List<OpOperation>> nonDeletedOpsByTypes = new HashMap<String, List<OpOperation>>();
		for(OpOperation o : operations.getQueueOperations()) {
			List<OpOperation> prevByType = nonDeletedOpsByTypes.get(o.getType());
			if(operationsToDelete.contains(o.getRawHash())) {
				deletedOps.add(o.getRawHash());
				atomicRemoveOperationObj(o, nonDeletedOpsByTypes.get(o.getType()));
			} else {
				if(prevByType == null) {
					prevByType = new ArrayList<OpOperation>();
					nonDeletedOpsByTypes.put(o.getType(), prevByType);
				}
				prevByType.add(o);
			}
		}
		operations.removeOperations(operationsToDelete);
		return deletedOps;
	}
	
	
	private void copyAndMergeWithParent(OpBlockChain copy, OpBlockChain parent ) {
		if(copy.isDbAccessed() || parent.isDbAccessed()) {
			throw new UnsupportedOperationException();
		}
		// 1. add blocks and their hashes
		blocks.copyAndMerge(copy.blocks, parent.blocks, parent.getSuperblocksDepth());
		
		// 2. merge operations cache with create delete info
		operations.copyAndMerge(copy.operations, parent.operations);
		
		// 3. merge named objects
		TreeSet<String> types = new TreeSet<String>(parent.objByName.keySet());
		types.addAll(copy.objByName.keySet());
		for(String type : types){
			OpPrivateObjectInstancesById nid = getOrCreateObjectsByIdMap(type);
			OpPrivateObjectInstancesById cid = copy.objByName.get(type);
			OpPrivateObjectInstancesById pid = parent.objByName.get(type);
			nid.putObjects(pid, true);
			nid.putObjects(cid, true);
			
		}
	}
	
	private void atomicRemoveOperationObj(OpOperation op, List<OpOperation> prevOperationsSameType) {
		// delete new objects by name
		for (OpObject ok : op.getNew()) {
			List<String> id = ok.getId();
			if (id != null && id.size() > 0) {
				String objType = op.getType();
				OpPrivateObjectInstancesById oinf = getOrCreateObjectsByIdMap(objType);
				OpObject currentObj = oinf.getObjectById(id);
				if (ok.equals(currentObj)) {
					OpObject p = null;
					if (prevOperationsSameType != null) {
						p = findLast(prevOperationsSameType, id);
					}
					oinf.add(id, p);
				}
			}
		}
	}
	
	private OpObject findLast(List<OpOperation> list, List<String> id) {
		OpObject last = null;
		for(OpOperation o : list) {
			for(OpObject obj : o.getNew()) {
				if(OUtils.equals(obj.getId(), id)) {
					last = obj;
				}
			}
		}
		return last;
	}

	
	public Deque<OpOperation> getQueueOperations() {
		return operations.getQueueOperations();
	}
	
	public OpBlockChain getParent() {
		return parent;
	}
	
	public OpBlockchainRules getRules() {
		return rules;
	}
	
	public OpBlock getLastBlockHeader() {
		if(nullObject) {
			return null;
		}
		OpBlock h = blocks.getLastBlockHeader();
		if(h == null) {
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
	
	public Collection<OperationDeleteInfo> getSuperblockDeleteInfo() {
		return operations.getOperationInfos();
	}
	
	public Map<String, Map<CompoundKey, OpObject>> getSuperblockObjects() {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		Map<String, Map<CompoundKey, OpObject>> mp = new TreeMap<String, Map<CompoundKey, OpObject>>(); 
		for(String type : objByName.keySet()) {
			OpPrivateObjectInstancesById bid = objByName.get(type);
			Map<CompoundKey, OpObject> allObjects = bid.getAllObjects();
			mp.put(type, allObjects);
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
		if(parent.getLastBlockId() > id) {
			return parent.getBlockHeadersById(id);
		}
		for(OpBlock o : blocks.getAllBlockHeaders()) {
			if(o.getBlockId() == id) {
				return o;
			}
		}
		return null;
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
	
	public OpBlock getFullBlockByRawHash(String hash) {
		if(nullObject) {
			return null;
		}
		OpBlock n = blocks.getFullBlockByHash(hash);
		if(n != null) {
			return n;
		}
		return parent.getFullBlockByRawHash(hash);
	}
	
	public OpOperation getOperationByHash(String rawHash) {
		if(nullObject) {
			return null;
		}
		// to do: this method is not optimal cause we can query in db by raw hash much quicker through all parents
		OperationDeleteInfo odi = operations.getOperationInfo(rawHash);
		if(odi != null) {
			return odi.op;
		}
		return parent.getOperationByHash(rawHash);
	}
	
	public OpObject getObjectByName(String type, String key) {
		return getObjectByName(type, key, null);
	}
	
	public OpObject getObjectByName(String type, String key, String secondary) {
		if (isNullBlock()) {
			return null;
		}
		OpPrivateObjectInstancesById ot = getOrCreateObjectsByIdMap(type);
		if (ot != null) {
			OpObject obj = ot.getObjectById(key, secondary);
			if (obj != null) {
				return obj;
			}
		}
		return parent.getObjectByName(type, key, secondary);
	}
	
	public OpObject getObjectByName(String type, List<String> o) {
		if (isNullBlock()) {
			return null;
		}
		OpPrivateObjectInstancesById ot = getOrCreateObjectsByIdMap(type);
		if (ot != null) {
			OpObject obj = ot.getObjectById(o);
			if (obj != null) {
				return obj;
			}
		}
		return parent.getObjectByName(type, o);
	}
	
	
	public void setCacheAfterSearch(ObjectsSearchRequest request, Object cacheObject) {
		if(request.objToSetCache != null) {
			request.objToSetCache.setCacheObject(cacheObject, request.editVersion);
		}
	}

	public void getObjects(String type, ObjectsSearchRequest request) {
		if(isNullBlock()) {
			return;
		}
		OpPrivateObjectInstancesById oi = getOrCreateObjectsByIdMap(type);
		if(oi == null) {
			parent.getObjects(type, request);
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
			fetchAllObjects(type, request);
		}
	}
	
	private void fetchAllObjects(String type, ObjectsSearchRequest request) {
		if(isNullBlock()) {
			return;
		}
		OpPrivateObjectInstancesById o = getOrCreateObjectsByIdMap(type);
		if(o != null) {
			o.fetchAllObjects(request);
		}
		if(request.limit == -1 || request.result.size() < request.limit) {
			parent.fetchAllObjects(type, request);
		}
	}
	
	
	private OpPrivateObjectInstancesById getOrCreateObjectsByIdMap(String type) {
		// create is allowed only when status is not locked
		if(nullObject) {
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
	
	
	
	private OperationDeleteInfo getOperationInfo(String hash) {
		if(nullObject) {
			return null;
		}
		OperationDeleteInfo cdi = operations.getOperationInfo(hash);
		if(cdi != null && cdi.create) {
			return cdi;
		}
		OperationDeleteInfo pdi = parent.getOperationInfo(hash);
		if(cdi != null && pdi != null) {
			return OpPrivateOperations.mergeDeleteInfo(cdi, pdi);
		} else if(cdi != null) {
			return cdi;
		} 
		return pdi;
	}

	
	private boolean validateAndPrepareOperation(OpOperation u, LocalValidationCtx ctx) {
		ValidationTimer vld = new ValidationTimer().start();
		if(OUtils.isEmpty(u.getRawHash())) {
			return rules.error(u, ErrorType.OP_HASH_IS_NOT_CORRECT, u.getHash(), "");
		}
		OperationDeleteInfo oin = getOperationInfo(u.getRawHash());
		if(oin != null) {
			return rules.error(u, ErrorType.OP_HASH_IS_DUPLICATED, u.getHash(), ctx.blockHash);
		}
		u.updateObjectsRef();
		boolean valid = true;
		valid = prepareDeletedObjects(u, ctx);
		if(!valid) {
			return false;
		}
		// should be called after prepareDeletedObjects (so cache is prepared)
		valid = prepareNoNewDuplicatedObjects(u, ctx);
		if(!valid) {
			return false;
		}
		valid = prepareReferencedObjects(u, ctx);
		if(!valid) {
			return valid;
		}
		vld.measure(ValidationTimer.OP_PREPARATION);
		valid = rules.validateOp(this, u, ctx.deletedObjsCache, ctx.refObjsCache, vld);
		if(!valid) {
			return valid;
		}
		vld.measure(ValidationTimer.OP_VALIDATION);
		u.putCacheObject(OpObject.F_VALIDATION, vld.getTimes());
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
		}

		if (getObjectByName(OpBlockchainRules.OP_OPERATION, u.getType()) != null)
			ctx.refObjsCache.put("op", getObjectByName(OpBlockchainRules.OP_OPERATION, u.getType()));
		return true;
	}
	
	private boolean prepareDeletedObjects(OpOperation u, LocalValidationCtx ctx) {
		List<String> deletedRefs = u.getOld();
		ctx.deletedObjsCache.clear();
		ctx.deletedOpsCache.clear();
		for(int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = getHashFromAbsRef(delRef);
			int delInd = getIndexFromAbsRef(delRef);
			
			OperationDeleteInfo opInfo = getOperationInfo(delHash);
			if(opInfo == null || opInfo.op.getNew().size() <= delInd) {
				return rules.error(u, ErrorType.DEL_OBJ_NOT_FOUND, u.getHash(), delRef);
			}
			if(opInfo.deletedObjects != null && delInd < opInfo.deletedObjects.length){
				if(opInfo.deletedObjects[delInd]) {
					return rules.error(u, ErrorType.DEL_OBJ_DOUBLE_DELETED, u.getHash(), 
							delRef, ctx.blockHash);
				}
			}
			List<OpObject> nw = opInfo.op.getNew();
			ctx.deletedObjsCache.add(nw.get(delInd));
			ctx.deletedOpsCache.add(opInfo.op);
		}
		return true;
	}
	
	private boolean prepareNoNewDuplicatedObjects(OpOperation u, LocalValidationCtx ctx) {
		List<OpObject> list = u.getNew();
		for(int i = 0; i < list.size(); i++) {
			OpObject o = list.get(i);
			// check duplicates in same operation
			for(int j = 0; j < i; j++) {
				OpObject oj = list.get(j);
				if(OUtils.equals(oj.getId(), o.getId())) {
					return rules.error(u, ErrorType.NEW_OBJ_DOUBLE_CREATED, u.getHash(), 
							o.getId());
				}
			}
			boolean newVersion = false;
			for(OpObject del : ctx.deletedObjsCache) {
				if(OUtils.equals(del.getId(), o.getId())) {
					newVersion = true;
					break;
				}
			}
			if(!newVersion) {
				OpObject exObj = getObjectByName(u.getType(), o.getId());
				if(exObj != null) {
					return rules.error(u, ErrorType.NEW_OBJ_DOUBLE_CREATED, u.getHash(), 
							o.getId());	
				}
			}
		}
		return true;
	}

	// no multi thread issue (used only in synchronized blocks)
	private static class LocalValidationCtx {
		final String blockHash;
		Map<String, OpObject> refObjsCache = new HashMap<String, OpObject>();
		List<OpObject> deletedObjsCache = new ArrayList<OpObject>();
		List<OpOperation> deletedOpsCache = new ArrayList<OpOperation>();

		public LocalValidationCtx(String bhash) {
			blockHash = bhash;
		}
	}
	
	public interface BlockDbAccessInterface {

		OpObject getObjectById(String type, CompoundKey k);

		Map<CompoundKey, OpObject> getAllObjects(String type, ObjectsSearchRequest request);

		OperationDeleteInfo getOperationInfo(String rawHash);

		Deque<OpBlock> getAllBlocks(Collection<OpBlock> blockHeaders);

		OpBlock getBlockByHash(String rawHash);

	}
	
	public static class ObjectsSearchRequest {
		public int editVersion;
		public int limit = -1;
		public boolean requestCache = false;
		
		public List<OpObject> result = new ArrayList<OpObject>();
		public int cacheVersion = -1;
		public Object cacheObject;
		
		Object internalMapToFilterDuplicates; 
		OpPrivateObjectInstancesById objToSetCache;
	}


}
