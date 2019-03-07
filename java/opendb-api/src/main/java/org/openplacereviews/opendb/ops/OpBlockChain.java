package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
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
	
	
	private static final int LOCKED_ERROR = -1;
	private static final int LOCKED_SUCCESS =  1;
	private static final int UNLOCKED =  0;
	// check SimulateSuperblockCompactSequences to verify numbers
	private static final double COMPACT_COEF = 0.5;
	private int locked = UNLOCKED; // 0, -1 error, 1 intentional

	// 0. parent chain
	private OpBlockChain parent;
	// 1. list of blocks
	private final Deque<OpBlock> blocks = new ConcurrentLinkedDeque<OpBlock>();
	// 2. block hash ids link to blocks
	private final Map<String, Integer> blockDepth = new ConcurrentHashMap<>();
	// 3. operations to be stored like a queue 
	private final Deque<OpOperation> operations = new ConcurrentLinkedDeque<OpOperation>();
	// 4. stores information about created and deleted objects in this blockchain 
	private final Map<String, OperationDeleteInfo> opsByHash = new ConcurrentHashMap<>();
	// 5. stores information about last object by name in this blockchain
	private final Map<String, ObjectInstancesById> objByName = new ConcurrentHashMap<>();

	
	public OpBlockChain(OpBlockChain parent) {
		this.parent = parent;
		if(this.parent != null) {
			this.parent.makeImmutable();
		}
	}
	
	public synchronized void makeImmutable() {
		if(this.locked == UNLOCKED) {
			this.locked = LOCKED_SUCCESS;
		} else if(this.locked != LOCKED_SUCCESS) {
			throw new IllegalStateException("This chain is locked with a broken state");
		}
	}
	
	public synchronized OpBlock createBlock(OpBlockchainRules rules) throws FailedVerificationException {
		validateIsUnlocked();
		OpBlock block = rules.createAndSignBlock(operations, getLastBlock());
		boolean valid = rules.validateBlock(this, block, getLastBlock());
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
		
		OpBlock lb = parent == null ? null : parent.getLastBlock();
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
		this.parent = newParent;
		this.blocks.clear();
		this.blockDepth.clear();
	}
	
	public synchronized boolean mergeWithParent() {
		// no need to change that state is not locked
		if(parent != null) {
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
		// 0. change parent
		parent = newParent;
	}
	
	public synchronized boolean compact() {
		// synchronized cause internal objects could be changed
 
		if(parent != null && parent.parent != null) {
			if(COMPACT_COEF * (parent.getSubchainSize()  + getSubchainSize()) >= parent.parent.getSubchainSize() ) {
				parent.mergeWithParent();
			}
			parent.compact();
		}
		return true;
		
		
	}
	
	private void validateIsUnlocked() {
		if(locked != UNLOCKED) {
			throw new IllegalStateException("This chain is immutable");
		}
	}

	private void atomicRemoveOperation(OpOperation op, List<OpOperation> prevOperationsSameType) {
		// delete operation itself
		OperationDeleteInfo odi = opsByHash.get(op.getRawHash());
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
	public synchronized boolean addOperation(OpOperation op, OpBlockchainRules rules) {
		validateIsUnlocked();
		LocalValidationCtx validationCtx = new LocalValidationCtx();
		validationCtx.rules = rules;
		validationCtx.blockHash = "";
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
	
	public OpBlock getLastBlock() {
		if(blocks.size() == 0) {
			return parent == null ? null : parent.getLastBlock();
		}
		return blocks.peekFirst();
	}
	
	public int getLastBlockId() {
		OpBlock o = getLastBlock();
		return o != null ? o.getBlockId() : -1;
	}
	
	public int getDepth() {
		return getLastBlockId();
	}
	
	public Deque<OpBlock> getBlocks() {
		return blocks;
	}
	
	public int getSubchainSize() {
		return blocks.size();
	}
	
	public OpBlock getBlockById(int blockId) {
		int last = getLastBlockId();
		if(last < blockId) {
			return null;
		}
		int first = last - getSubchainSize() + 1;
		if(first <= blockId) {
			int it = last - blockId; 
			Iterator<OpBlock> its = blocks.iterator();
			while(--it > 0) {
				its.next();
			}
			return its.next();
		}
		return parent == null ? null : parent.getBlockById(blockId); 
	}
	

	public int getBlockDepth(String hash) {
		Integer n = blockDepth.get(hash);
		if(n != null) {
			return n;
		}
		if(parent != null) {
			return parent.getBlockDepth(hash);
		}
		return -1;
	}
	
	
	private ObjectInstancesById getObjectsByIdMap(String type, boolean create) {
		ObjectInstancesById oi = objByName.get(type);
		if(oi == null) {
			ObjectInstancesById pi = parent == null ? null : parent.getObjectsByIdMap(type, true);
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
				pi.deletedObjects = new boolean[pi.op.getNew().size()];
				opsByHash.put(delHash, pi);
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
		if(getLastBlockId() <= maxdepth && maxdepth != -1) {
			return null;
		}
		OperationDeleteInfo cdi = opsByHash.get(hash);
		if(cdi != null && cdi.create) {
			return cdi;
		}
		OperationDeleteInfo pdi = null;
		if(parent != null) {
			pdi = parent.getOperationInfo(hash, maxdepth);
		}
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
		OperationDeleteInfo oin = getOperationInfo(u.getRawHash(), -1);
		if(oin != null) {
			return ctx.rules.error(ErrorType.OP_HASH_IS_DUPLICATED, u.getRawHash(), ctx.blockHash);
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
		valid = ctx.rules.validateOp(this, u, ctx.deletedObjsCache, ctx.refObjsCache);
		if(!valid) {
			return valid;
		}
		return true;
	}
	
	
	
	private boolean prepareReferencedObjects(OpOperation u, LocalValidationCtx ctx) {
		Map<String, List<String>> refs = u.getRef();
		if(refs == null) {
			return true;
		}
		Iterator<Entry<String, List<String>>> it = refs.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, List<String>> e = it.next();
			String refName = e.getKey();
			List<String> refObjName = e.getValue();
			OpObject oi = null;
			if(refObjName.size() > 1) {
				// type is necessary
				OpBlockChain blc = this;
				String objType = refObjName.get(0);
				List<String> refKey = refObjName.subList(1, refObjName.size());
				while (blc != null && oi == null) {
					oi = blc.getObjectByName(objType, refKey);
					blc = blc.parent;
				}
			}
			if(oi == null) {
				return ctx.rules.error(ErrorType.REF_OBJ_NOT_FOUND, u.getRawHash(), refObjName);
			}
			ctx.refObjsCache.put(refName, oi);
		}
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
				return ctx.rules.error(ErrorType.DEL_OBJ_NOT_FOUND, u.getRawHash(), delRef);
			}
			if(opInfo.deletedObjects != null || delInd < opInfo.deletedObjects.length){
				if(opInfo.deletedObjects[delInd]) {
					return ctx.rules.error(ErrorType.DEL_OBJ_DOUBLE_DELETED, u.getRawHash(), 
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
					return ctx.rules.error(ErrorType.NEW_OBJ_DOUBLE_CREATED, u.getRawHash(), 
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
					return ctx.rules.error(ErrorType.NEW_OBJ_DOUBLE_CREATED, u.getRawHash(), 
							o.getId());	
				}
			}
		}
		return true;
	}



	// no multi thread issue (used only in synchronized blocks)
	private static class LocalValidationCtx {
		Map<String, OpObject> refObjsCache = new HashMap<String, OpObject>();
		List<OpObject> deletedObjsCache = new ArrayList<OpObject>();
		List<OpOperation> deletedOpsCache = new ArrayList<OpOperation>();
		
		String blockHash;
		OpBlockchainRules rules;
	}

	private static class OperationDeleteInfo {
		private OpOperation op;
		private boolean create;
		private boolean[] deletedObjects;
	}


}
