package org.openplacereviews.opendb.ops;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.ObjectInstancesById.CacheObject;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;

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
	
	
	public static final int LOCKED_ERROR = -1;
	public static final int LOCKED_SUCCESS = 1;
	public static final int LOCKED_BY_USER = 2;
	public static final int UNLOCKED =  0;
	public static final OpBlockChain NULL = new OpBlockChain(true);
	private volatile int locked = UNLOCKED; // 0, -1 error, 1 intentional

	// 0.0 nullable object is always root (in order to perform operations in sync)
	private final boolean nullObject;
	// 0.1 immutable blockchain rules to validate operations
	private final OpBlockchainRules rules;
	// 0.2 parent chain
	private volatile OpBlockChain parent;
	
	// 1. list of blocks
	private final Deque<OpBlock> blocks = new ConcurrentLinkedDeque<OpBlock>();
	// 2. block hash ids link to blocks
	private final Map<String, Integer> blockDepth = new ConcurrentHashMap<>();
	
	// These objects should be stored on disk (DB)
	// 3. operations to be stored like a queue 
	private final Deque<OpOperation> operations = new ConcurrentLinkedDeque<OpOperation>();
	// 4. stores information about created and deleted objects in this blockchain 
	private final Map<String, OperationDeleteInfo> opsByHash = new ConcurrentHashMap<>();
	// 5. stores information about last object by name in this blockchain
	private final Map<String, ObjectInstancesById> objByName = new ConcurrentHashMap<>();
	
	
	private OpBlockChain(boolean nullParent) {
		this.nullObject = true;
		this.rules = null;
		locked = LOCKED_SUCCESS;
	}
	
	public OpBlockChain(OpBlockChain parent, OpBlockchainRules rules) {
		this.rules = rules;
		this.nullObject = false;
		if(parent == null) {
			throw new IllegalStateException("Parent can not be null, use null object for reference");
		}
		atomicSetParent(parent);
	}
	
	public OpBlockChain(OpBlockChain copy, OpBlockChain parentToMerge, OpBlockchainRules rules) {
		this.rules = rules;
		this.nullObject = false;
		if(parentToMerge == null || parentToMerge.isNullBlock() || copy.parent != parentToMerge) {
			throw new IllegalStateException("Wrong parameters to create object with merged parents");
		}
		copy.validateLocked();
		parentToMerge.validateLocked();
		
		atomicSetParent(parentToMerge.parent);
		copyAndMergeWithParent(copy);
		
	}
	
	private synchronized void validateLocked() {
		if(nullObject) {
			return;
		}
		if(this.locked == UNLOCKED) {
			this.locked = LOCKED_SUCCESS;
		} else if(this.locked != LOCKED_SUCCESS) {
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
		OpBlock block = rules.createAndSignBlock(operations, getLastBlock(), user, keyPair);
		validateIsUnlocked();
		boolean valid = rules.validateBlock(this, block, getLastBlock(), true);
		if(!valid) {
			return null;
		}
		String blockHash = block.getHash();
		int blockId = block.getBlockId();
		locked = LOCKED_SUCCESS;
		try {
			atomicCreateBlockFromAllOps(block, blockHash, blockId);
			locked = UNLOCKED;
		} finally {
			if(locked == LOCKED_SUCCESS) {
				locked = LOCKED_ERROR;
			}
		}
		return block;
	}

	public synchronized OpBlock replicateBlock(OpBlock block) {
		block.checkImmutable();
		validateIsUnlocked();
		if (!operations.isEmpty()) {
			// can't replicate blocks when operations are not empty
			return null;
		}
		OpBlock prev = getLastBlock();
		boolean valid = rules.validateBlock(this, block, prev, block.getBlockId() != 0);
		if (!valid) {
			return null;
		}
		locked = LOCKED_SUCCESS;
		try {
			for (OpOperation o : block.getOperations()) {
				LocalValidationCtx validationCtx = new LocalValidationCtx(block.getHash());
				validateAndPrepareOperation(o, validationCtx);
				atomicAddOperationAfterPrepare(o, validationCtx);
			}
			atomicCreateBlockFromAllOps(block, block.getHash(), block.getBlockId());
			locked = UNLOCKED;
		} finally {
			if (locked == LOCKED_SUCCESS) {
				locked = LOCKED_ERROR;
			}
		}
		return block;
	}
	
	public synchronized boolean rebaseOperations(OpBlockChain newParent) {
		validateIsUnlocked();
		newParent.validateLocked();
		if(!newParent.operations.isEmpty()) {
			return false;
		}
		// calculate blocks and ops to be removed, all blocks must be present in new parent
		// if(blocks.size() > 0) { return false; }
		for(OpBlock bl : blocks) {
			int blDept = newParent.getBlockDepth(bl.getHash());
			if(blDept < 0) {
				return false;
			}
		}
		
		OpBlock lb = parent.getLastBlock();
		if(lb != null && newParent.getBlockDepth(lb.getHash()) == -1) {
			// rebase is not allowed
			return false;
		}
		locked = LOCKED_SUCCESS;
		try {
			atomicRebaseOperations(newParent);
			locked = UNLOCKED;
		} finally {
			if(locked == LOCKED_SUCCESS) {
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
		if(!OUtils.equals(newParent.getLastHash(), parent.getLastHash())) {
			return false;
		}
		// operation doesn't require locking mechanism
		int status = locked;
		if(status != LOCKED_SUCCESS || status != UNLOCKED) {
			return false;
		}
		locked = LOCKED_SUCCESS;
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
	
	/**
	 * Adds operation and validates it to block chain
	 */
	public synchronized boolean addOperation(OpOperation op) {
		op.checkImmutable();
		validateIsUnlocked();
		LocalValidationCtx validationCtx = new LocalValidationCtx("");
		boolean valid = validateAndPrepareOperation(op, validationCtx);
		if(!valid) {
			return valid;
		}
		locked = LOCKED_SUCCESS;
		try {
			atomicAddOperationAfterPrepare(op, validationCtx);
			locked = UNLOCKED;
		} finally {
			if(locked == LOCKED_SUCCESS) {
				locked = LOCKED_ERROR;
			}
		}
		return valid;
	}
	
	private void atomicAddOperationAfterPrepare(OpOperation u, LocalValidationCtx validationCtx) {
		for (OpObject newObj : u.getNew()) {
			List<String> id = newObj.getId();
			if (id != null && id.size() > 0) {
				String objType = u.getType();
				ObjectInstancesById oinf = getOrCreateObjectsByIdMap(objType);
				oinf.add(id, newObj);
			}
		}
		List<String> deletedRefs = u.getOld();
		for(int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = getHashFromAbsRef(delRef);
			int delInd = getIndexFromAbsRef(delRef);
			OperationDeleteInfo pi = opsByHash.get(delHash);
			if(pi == null) {
				pi = new OperationDeleteInfo();
				pi.op = validationCtx.deletedOpsCache.get(i);
				opsByHash.put(delHash, pi);
			}
			if(pi.deletedObjects == null) {
				pi.deletedObjects = new boolean[pi.op.getNew().size()];
			}
			pi.deletedObjects[delInd] = true;
		}
		OperationDeleteInfo infop = new OperationDeleteInfo();
		infop.op = u;
		infop.create = true;
		opsByHash.put(u.getRawHash(), infop);
		
		operations.add(u);
	}
	
	private void atomicSetParent(OpBlockChain parent) {
		if(!parent.isNullBlock()) {
			if(this.rules != parent.rules) {
				throw new IllegalStateException("Blockchain rules should be consistent trhough whole chain");
			}
		}
		parent.validateLocked();
		if(!parent.operations.isEmpty()) {
			throw new IllegalStateException("Parent chain doesn't allow to have operations");
		}
		this.parent = parent;
	}

	private void atomicCreateBlockFromAllOps(OpBlock block, String blockHash, int blockId) {
		operations.clear();
		blockDepth.put(blockHash, blockId);
		blocks.push(block);
	}

	private void atomicRebaseOperations(OpBlockChain newParent) {
		// all blocks must be present in new parent
		for(OpBlock b : blocks) {
			for(OpOperation o : b.getOperations()) {
				atomicRemoveOperation(o, null);
			}
		}
		Map<String, List<OpOperation>> nonDeletedOpsByTypes = new HashMap<String, List<OpOperation>>();
		Iterator<OpOperation> it = operations.iterator();
		while(it.hasNext()) {
			OpOperation o = it.next();
			OperationDeleteInfo odi = newParent.getOperationInfo(o.getRawHash());
			List<OpOperation> prevByType = nonDeletedOpsByTypes.get(o.getType());
			if(odi != null && odi.create) {
				it.remove();
				atomicRemoveOperation(o, nonDeletedOpsByTypes.get(o.getType()));
			} else {
				if(prevByType == null) {
					prevByType = new ArrayList<OpOperation>();
					nonDeletedOpsByTypes.put(o.getType(), prevByType);
				}
				prevByType.add(o);
			}
		}
		atomicSetParent(newParent);
		this.blocks.clear();
		this.blockDepth.clear();
	}
	
	
	private void copyAndMergeWithParent(OpBlockChain copy) {
		OpBlockChain parent = copy.parent;
		// 1, 2. add blocks and their hashes
		blocks.addAll(copy.blocks);
		blocks.addAll(parent.blocks);
		
		blockDepth.putAll(copy.blockDepth);
		blockDepth.putAll(parent.blockDepth);
		
		// 3. merge operations cache with create delete info
		TreeSet<String> setOfOperations = new TreeSet<String>(parent.opsByHash.keySet());
		setOfOperations.addAll(copy.opsByHash.keySet());
		for (String operation : setOfOperations) {
			OperationDeleteInfo cp = copy.opsByHash.get(operation);
			OperationDeleteInfo pp = parent.opsByHash.get(operation);
			this.opsByHash.put(operation, mergeDeleteInfo(cp, pp));
		}
		// 4. merge named objects
		TreeSet<String> types = new TreeSet<String>(parent.objByName.keySet());
		types.addAll(copy.objByName.keySet());
		for(String type : types){
			ObjectInstancesById nid = getOrCreateObjectsByIdMap(type);
			ObjectInstancesById cid = copy.objByName.get(type);
			ObjectInstancesById pid = parent.objByName.get(type);
			nid.putObjects(pid, true);
			nid.putObjects(cid, true);
			
		}
	}
	
	private void atomicRemoveOperation(OpOperation op, List<OpOperation> prevOperationsSameType) {
		// delete operation itself
		OperationDeleteInfo odi = opsByHash.remove(op.getRawHash());
		odi.create = false;
		// delete deleted objects by name
		List<String> deletedRefs = op.getOld();
		for (int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = getHashFromAbsRef(delRef);
			int delInd = getIndexFromAbsRef(delRef);
			OperationDeleteInfo pi = opsByHash.get(delHash);
			if (pi != null && pi.deletedObjects.length > delInd) {
				pi.deletedObjects[delInd] = false;
			}
		}

		// delete new objects by name
		for (OpObject ok : op.getNew()) {
			List<String> id = ok.getId();
			if (id != null && id.size() > 0) {
				String objType = op.getType();
				ObjectInstancesById oinf = getOrCreateObjectsByIdMap(objType);
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

	
	public Collection<OpOperation> getOperations() {
		return operations;
	}
	
	public OpBlockChain getParent() {
		return parent;
	}
	
	public OpBlockchainRules getRules() {
		return rules;
	}
	
	public OpBlock getLastBlock() {
		if(nullObject) {
			return null;
		}
		if(blocks.size() == 0) {
			return parent.getLastBlock();
		}
		return blocks.peekFirst();
	}
	
	public String getLastHash() {
		OpBlock b = getLastBlock();
		return b == null ? "" : b.getHash();
	}
	
	public int getLastBlockId() {
		OpBlock o = getLastBlock();
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
	
	public LinkedHashMap<String, List<OpBlock>> getBlocksBySuperBlocks(int depth, LinkedHashMap<String, List<OpBlock>> mp) {
		if(mp == null) {
			mp = new LinkedHashMap<String, List<OpBlock>>(); 
		}
		if(nullObject) {
			return mp;
		}
		mp.put(getSuperBlockHash(), new ArrayList<OpBlock>(blocks));
		return parent.getBlocksBySuperBlocks(depth, mp);
	}
	
	public List<OpBlock> getBlocks(int depth) {
		List<OpBlock> lst = new ArrayList<>();
		fetchBlocks(lst, depth);
		return lst;
	}
	
	
	public String getSuperBlockHash() {
		if (blocks.size() == 0) {
			return "";
		}
		String hsh = getLastHash();
		int i = hsh.lastIndexOf(':');
		if (i >= 0) {
			hsh = hsh.substring(i + 1);
		}
		return rules.calculateSuperblockHash(blocks.size(), hsh);
	}
	
	public Collection<OpBlock> getOneSuperBlock() {
		return blocks;
	}
	
	public int getSuperblockSize() {
		return blocks.size();
	}
	
	public OpBlock getBlockById(int blockId) {
		if(nullObject) {
			return null;
		}
		int last = getLastBlockId();
		if(last < blockId) {
			return null;
		}
		int first = last - getSuperblockSize() + 1;
		if(first <= blockId) {
			int it = last - blockId; 
			Iterator<OpBlock> its = blocks.iterator();
			while(--it > 0) {
				its.next();
			}
			return its.next();
		}
		return parent.getBlockById(blockId); 
	}
	

	public int getBlockDepth(String hash) {
		if(nullObject) {
			return -1;
		}
		Integer n = blockDepth.get(hash);
		if(n != null) {
			return n;
		}
		return parent.getBlockDepth(hash);
	}
	
	public OpObject getObjectByName(String type, String key) {
		return getObjectByName(type, key, null);
	}
	
	public OpObject getObjectByName(String type, String key, String secondary) {
		if (isNullBlock()) {
			return null;
		}
		ObjectInstancesById ot = objByName.get(type);
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
		ObjectInstancesById ot = objByName.get(type);
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

	public void fetchObjects(String type, ObjectsSearchRequest request) {
		if(isNullBlock()) {
			return;
		}
		ObjectInstancesById oi = objByName.get(type);
		if(oi == null) {
			parent.fetchObjects(type, request);
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
		ObjectInstancesById o = objByName.get(type);
		if(o != null) {
			o.fetchAllObjects(request);
		}
		if(request.limit == -1 || request.result.size() < request.limit) {
			parent.fetchAllObjects(type, request);
		}
	}
	
	
	private ObjectInstancesById getOrCreateObjectsByIdMap(String type) {
		// create is allowed only when status is not locked
		if(nullObject) {
			return null;
		}
		ObjectInstancesById oi = objByName.get(type);
		if(oi == null) {
			oi = new ObjectInstancesById(type);
			objByName.put(type, oi);
		}
		return oi;
	}
	
	private String getHashFromAbsRef(String r) {
		int i = r.indexOf(':');
		if(i == -1) {
			return r;
		}
		return r.substring(0, i);
	}
	
	private void fetchBlocks(List<OpBlock> lst, int depth) {
		if(nullObject) {
			return;
		}
		lst.addAll(blocks);
		depth -= blocks.size();
		if(depth > 0) {
			parent.fetchBlocks(lst, depth);
		}
	}
	
	private int getIndexFromAbsRef(String r) {
		int i = r.indexOf(':');
		if (i == -1) {
			return 0;
		}
		return Integer.parseInt(r.substring(i + 1));
	}
	
	private OperationDeleteInfo getOperationInfo(String hash) {
		if(nullObject) {
			return null;
		}
		OperationDeleteInfo cdi = opsByHash.get(hash);
		if(cdi != null && cdi.create) {
			return cdi;
		}
		OperationDeleteInfo pdi = parent.getOperationInfo(hash);
		if(cdi != null && pdi != null) {
			return mergeDeleteInfo(cdi, pdi);
		} else if(cdi != null) {
			return cdi;
		} 
		return pdi;
	}

	private OperationDeleteInfo mergeDeleteInfo(OperationDeleteInfo cdi, OperationDeleteInfo pdi) {
		OperationDeleteInfo ndi = new OperationDeleteInfo();
		ndi.create = (pdi != null && pdi.create) || (cdi != null && cdi.create);
		if((pdi != null && pdi.create) && (cdi != null && cdi.create)) {
			// assert
			throw new IllegalArgumentException("Operation was created twice");
		}
		int psz = (pdi != null && pdi.deletedObjects == null) ? 0 : pdi.deletedObjects.length;
		int sz = (cdi != null && cdi.deletedObjects == null) ? 0 : cdi.deletedObjects.length;
		int length = Math.max(sz, psz);
		ndi.deletedObjects = new boolean[length];
		for(int i = 0; i < length; i++) {
			if(pdi != null && pdi.deletedObjects != null && i < pdi.deletedObjects.length && pdi.deletedObjects[i]) {
				ndi.deletedObjects[i] = true;
			}
			if(cdi != null && cdi.deletedObjects != null && i < cdi.deletedObjects.length && cdi.deletedObjects[i]) {
				if(ndi.deletedObjects[i]) {
					// assert
					throw new IllegalArgumentException("Object was deleted twice");
				}
				ndi.deletedObjects[i] = true;
			}
		}
		return ndi;
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
							delRef, opInfo.deletedObjects[delInd]);
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
	
	public static class ObjectsSearchRequest {
		public int editVersion;
		public int limit = -1;
		public boolean requestCache = false;
		
		public List<OpObject> result = new ArrayList<OpObject>();
		public int cacheVersion = -1;
		public Object cacheObject;
		
		Object internalMapToFilterDuplicates; 
		ObjectInstancesById objToSetCache;
	}

	private static class OperationDeleteInfo {
		private OpOperation op;
		private boolean create;
		private boolean[] deletedObjects;
	}




}
