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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
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
 *  5. There are 5 atomic operations: 
 *      - atomicCreateBlockFromAllOps / createBlock - combines queue operations into a block
 *      - atomicChangeParent / changeParent - change parent blockchain (git rebase), used for replication between blockchains or resolving conflicts
 *      - atomicMergeWithParent / compact + mergeWithParent - used for compacting blockchain and create super blocks
 *      - atomicRemoveOperation / - - used in atomicChangeParent
 *      - atomicAddOperationAfterPrepare  / addOperation - used to add operation into a queue
 *
 */
public class OpBlockChain {
	
	
	public static final int LOCKED_ERROR = -1;
	public static final int LOCKED_SUCCESS = 1;
	public static final int LOCKED_BY_USER = 2;
	public static final int UNLOCKED =  0;
	public static final OpBlockChain NULL = new OpBlockChain(true);
	private int locked = UNLOCKED; // 0, -1 error, 1 intentional

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

	private void atomicSetParent(OpBlockChain parent) {
		if(!parent.isNullBlock()) {
			if(this.rules != parent.rules) {
				throw new IllegalStateException("Blockchain rules should be consistent trhough whole chain");
			}
		}
		parent.makeImmutable();
		this.parent = parent;
	}
	
	public synchronized void makeImmutable() {
		if(nullObject) {
			return;
		}
		if(this.locked == UNLOCKED) {
			this.locked = LOCKED_BY_USER;
		} else if(this.locked != LOCKED_BY_USER) {
			throw new IllegalStateException("This chain is locked not by user or in a broken state");
		}
	}
	
	public synchronized void makeMutable() {
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
	
	public int getStatus() {
		return locked;
	}

	private void atomicCreateBlockFromAllOps(OpBlock block, String blockHash, int blockId) {
		operations.clear();
		blockDepth.put(blockHash, blockId);
		blocks.push(block);
	}
	
	public synchronized boolean changeParent(OpBlockChain newParent) {
		validateIsUnlocked();
		newParent.makeImmutable();
		// calculate blocks and ops to be removed all blocks must be present
		if(blocks.size() > 0) {
			return false;
		}
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
			atomicChangeParent(newParent, lb);
			locked = UNLOCKED;
		} finally {
			if(locked == LOCKED_SUCCESS) {
				locked = LOCKED_ERROR;
			}
		}
		return true;
	}

	private void atomicChangeParent(OpBlockChain newParent, OpBlock lastBlock) {
		// all blocks are present in new parent
		int depth = lastBlock == null ? -1 : lastBlock.getBlockId();
		for(OpBlock b : blocks) {
			for(OpOperation o : b.getOperations()) {
				atomicRemoveOperation(o, null);
			}
		}
		Map<String, List<OpOperation>> nonDeletedOpsByTypes = new HashMap<String, List<OpOperation>>();
		Iterator<OpOperation> it = operations.iterator();
		while(it.hasNext()) {
			OpOperation o = it.next();
			OperationDeleteInfo odi = newParent.getOperationInfo(o.getRawHash(), depth);
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
		for(ObjectInstancesById bid : this.objByName.values()) {
			ObjectInstancesById pid = newParent.getObjectsByIdMap(bid.getType(), true);
			bid.setParent(pid);
		}
		atomicSetParent(newParent);
		this.blocks.clear();
		this.blockDepth.clear();
	}
	
	public synchronized boolean mergeWithParent() {
		// no need to change that state is not locked
		if(!nullObject && !parent.nullObject) {
			locked = LOCKED_SUCCESS;
			try {
				atomicMergeWithParent();
				locked = UNLOCKED;
			} finally {
				if(locked == LOCKED_SUCCESS) {
					locked = LOCKED_ERROR;
				}
			}
		}
		return true;
	}

	private void atomicMergeWithParent() {
		OpBlockChain newParent = parent.parent;
		// 1, 2. add blocks and their hashes
		for(OpBlock last : parent.blocks) {
			this.blocks.addLast(last);
		}
		blockDepth.putAll(this.parent.blockDepth);
		
		// 3. merge queue operations
		Iterator<OpOperation> desc = operations.descendingIterator();
		while(desc.hasNext()) {
			operations.addFirst(desc.next());
		}
		// 4. merge operations cache with create delete info
		Iterator<Entry<String, OperationDeleteInfo>> deleteInfoIt = parent.opsByHash.entrySet().iterator();
		while(deleteInfoIt.hasNext()) {
			Entry<String, OperationDeleteInfo> e = deleteInfoIt.next();
			if(!this.opsByHash.containsKey(e.getKey())) {
				this.opsByHash.put(e.getKey(), e.getValue());
			} else {
				OperationDeleteInfo ndi = mergeDeleteInfo(this.opsByHash.get(e.getKey()), 
						e.getValue());
				this.opsByHash.put(e.getKey(), ndi);
			}
		}
		// 5. merge named objects 
		Iterator<Entry<String, ObjectInstancesById>> byNameIterator = parent.objByName.entrySet().iterator();
		while(byNameIterator.hasNext()) {
			Entry<String, ObjectInstancesById> e = byNameIterator.next();
			ObjectInstancesById exId = this.objByName.get(e.getKey());
			if(exId == null) {
				this.objByName.put(e.getKey(), e.getValue());
			} else {
				exId.mergeWithParent(e.getValue());
			}
		}
		// 6. change parent
		atomicSetParent(newParent);
	}
	
	private void validateIsUnlocked() {
		if(nullObject) {
			throw new IllegalStateException("This chain is immutable (null chain)");
		}
		if(locked != UNLOCKED) {
			throw new IllegalStateException("This chain is immutable");
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
				ObjectInstancesById oinf = getObjectsByIdMap(objType, true);
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
	
	
	ObjectInstancesById getObjectsByIdMap(String type, boolean create) {
		if(nullObject) {
			return null;
		}
		ObjectInstancesById oi = objByName.get(type);
		if(oi == null) {
			ObjectInstancesById pi = parent.getObjectsByIdMap(type, true);
			if(create) {
				oi = new ObjectInstancesById(type, pi);
				objByName.put(type, oi);
			} else {
				oi = pi;
			}
		}
		return oi;
	}
	
	public OpObject getObjectByName(String type, String key) {
		ObjectInstancesById ot = getObjectsByIdMap(type, false);
		if(ot == null) {
			return null;
		}
		return ot.getObjectById(key, null);
	}
	
	
	public List<OpObject> getObjects(String type, int limit) {
		List<OpObject> list = new ArrayList<OpObject>();
		fetchObjects(list, type, limit);
		return list;
	}
	
	
	private void fetchObjects(List<OpObject> lst, String type, int limit) {
		if(nullObject) {
			return;
		}
		ObjectInstancesById ot = objByName.get(type);
		if(ot != null) {
			Collection<OpObject> objects = ot.getObjects();
			lst.addAll(objects);
			limit -= objects.size();
		}
		if(limit > 0) {
			parent.fetchObjects(lst, type, limit);
		}
	}
	
	public OpObject getObjectByName(String type, String key, String secondary) {
		ObjectInstancesById ot = getObjectsByIdMap(type, false);
		if(ot == null) {
			return null;
		}
		return ot.getObjectById(key, secondary);
	}
	
	private OpObject getObjectByName(String objType, List<String> o) {
		ObjectInstancesById ot = getObjectsByIdMap(objType, false);
		if(ot == null) {
			return null;
		}
		return ot.getObjectById(o);
	}
	
	
	private String getHashFromAbsRef(String r) {
		int i = r.indexOf(':');
		if(i == -1) {
			return r;
		}
		return r.substring(0, i);
	}
	
	private int getIndexFromAbsRef(String r) {
		int i = r.indexOf(':');
		if (i == -1) {
			return 0;
		}
		return Integer.parseInt(r.substring(i + 1));
	}
	
	private void atomicAddOperationAfterPrepare(OpOperation u, LocalValidationCtx validationCtx) {
		for (OpObject newObj : u.getNew()) {
			List<String> id = newObj.getId();
			if (id != null && id.size() > 0) {
				String objType = u.getType();
				ObjectInstancesById oinf = getObjectsByIdMap(objType, true);
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
	
	private OperationDeleteInfo getOperationInfo(String hash, int maxdepth) {
		if(nullObject) {
			return null;
		}
		if(getLastBlockId() <= maxdepth && maxdepth != -1) {
			return null;
		}
		OperationDeleteInfo cdi = opsByHash.get(hash);
		if(cdi != null && cdi.create) {
			return cdi;
		}
		OperationDeleteInfo pdi = parent.getOperationInfo(hash, maxdepth);
		if(cdi != null && pdi != null) {
			return mergeDeleteInfo(cdi, pdi);
		} else if(cdi != null) {
			return cdi;
		} 
		return pdi;
	}

	private OperationDeleteInfo mergeDeleteInfo(OperationDeleteInfo cdi, OperationDeleteInfo pdi) {
		OperationDeleteInfo ndi = new OperationDeleteInfo();
		ndi.create = pdi.create || cdi.create;
		int psz = pdi.deletedObjects == null ? 0 : pdi.deletedObjects.length;
		int sz = cdi.deletedObjects == null ? 0 : cdi.deletedObjects.length;
		int length = Math.max(sz, psz);
		ndi.deletedObjects = new boolean[length];
		for(int i = 0; i < length; i++) {
			if(pdi.deletedObjects != null && i < pdi.deletedObjects.length && pdi.deletedObjects[i]) {
				ndi.deletedObjects[i] = true;
			}
			if(cdi.deletedObjects != null && i < cdi.deletedObjects.length && cdi.deletedObjects[i]) {
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
		OperationDeleteInfo oin = getOperationInfo(u.getRawHash(), -1);
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
			
			OperationDeleteInfo opInfo = getOperationInfo(delHash, -1);
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

	private static class OperationDeleteInfo {
		private OpOperation op;
		private boolean create;
		private boolean[] deletedObjects;
	}




}
