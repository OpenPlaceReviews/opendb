package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OperationInfo.DeleteInfo;

public class OpBlockChain {
	
	private final OpBlockChain parent;
	
	private Deque<OpBlock> blocks = new ConcurrentLinkedDeque<OpBlock>();
	private Map<String, Integer> blockDepth = new ConcurrentHashMap<>();
	private Map<String, OperationInfo> opsByHash = new ConcurrentHashMap<>();
	private Map<String, ObjectInfo> objByName = new ConcurrentHashMap<>();
	private boolean immutable;

	
	// no multi thread issue (used only in synchronized blocks)
	private Map<String, OpObject> refObjsCache = new HashMap<String, OpObject>();
	private List<OpObject> deletedObjsCache = new ArrayList<OpObject>();
	private List<OperationInfo> deletedObjInfoCache = new ArrayList<OperationInfo>();
	
	private static enum ErrorType {
		PREV_BLOCK_HASH("Previous block hash is not equal '%s' != '%s': block '%s'"),
		PREV_BLOCK_ID("Previous block id is not equal '%d' != '%d': block '%s'"),
		BLOCK_HASH_IS_DUPLICATED("Block hash is duplicated '%s' in block '%d' and '%d'"),
		OP_HASH_IS_DUPLICATED("Operation hash is duplicated '%s' in block '%d' and '%s'"),
		DEL_OBJ_NOT_FOUND("Object to delete wasn't found '%s': op '%s'"),
		REF_OBJ_NOT_FOUND("Object to reference wasn't found '%s': op '%s'"),
		DEL_OBJ_DOUBLE_DELETED("Object '%s' was already deleted at '%s' in block '%s'");
		private final String msg;

		ErrorType(String msg) {
			this.msg = msg;
		}
		
		public String getErrorFormat(Object... args) {
			return String.format(msg, args);
		}
	}
	

	public OpBlockChain(OpBlockChain parent, OpBlock b) {
		this.parent = parent;
		if(this.parent != null) {
			this.parent.makeImmutable();
		}
		addBlock(b);
	}
	
	
	public void makeImmutable() {
		this.immutable = true;
	}
	
	public synchronized boolean isImmutable() {
		return immutable;
	}
	

	public OpBlock getLastBlock() {
		if(blocks.size() == 0) {
			return parent == null ? null : parent.getLastBlock();
		}
		return blocks.peekFirst();
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
	
	
	public void error(ErrorType e, Object... args) {
		throw new IllegalArgumentException(e.getErrorFormat(args));
	}
	

	public synchronized void addBlock(OpBlock block) {
		if(immutable) {
			throw new IllegalStateException("Object is immutable");
		}
		OpBlock prevBlock = getLastBlock();
		int pid = -1;
		String blockHash = block.getHash();
		int blockId = block.getBlockId();
		if(prevBlock != null) {
			if(!OUtils.equals(prevBlock.getHash(), block.getStringValue(OpBlock.F_PREV_BLOCK_HASH))) {
				error(ErrorType.PREV_BLOCK_HASH, prevBlock.getHash(), 
						block.getStringValue(OpBlock.F_PREV_BLOCK_HASH), blockHash);
			}
			pid = prevBlock.getBlockId();
		}
		if(pid != blockId) {
			error(ErrorType.PREV_BLOCK_ID, pid, block.getBlockId(), blockHash);
		}
		int dupBl = getBlockDepth(block.getHash());
		if(dupBl != -1) {
			error(ErrorType.BLOCK_HASH_IS_DUPLICATED, blockHash, block.getBlockId(), dupBl);
		}
		
		List<OpOperation> ops = block.getOperations();
		
		for(OpOperation u : ops) {
			addOperation(blockHash, blockId, u);
		}
		blockDepth.put(blockHash, blockId);
		blocks.push(block);
	}


	private void addOperation(String blockHash, int blockId, OpOperation u) {
		validateAndPrepareOperation(blockHash, blockId, u);
		
		for(OpObject newObj : u.getNew()){
			List<String> id = newObj.getId();
			if(id != null && id.size() > 0) {
				String objType = id.get(0);
				ObjectInfo oinf = getObjectsByType(objType, true);
				oinf.add(id, newObj);
			}
		}
		List<String> deletedRefs = u.getOld();
		for(int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			int delInd = getIndexFromAbsRef(delRef);
			deletedObjInfoCache.get(i).addDelInfo(delInd, u, blockId, blockHash);
		}
		opsByHash.put(u.getHash(), new OperationInfo(u, this, blockId));
	}


	private synchronized void validateAndPrepareOperation(String blockHash, int blockId, OpOperation u) {
		OperationInfo oin = getOperationInfo(u.getHash());
		if(oin != null) {
			error(ErrorType.OP_HASH_IS_DUPLICATED, u.getHash(), oin.getBlockId(), blockHash);
		}
		prepareDeletedObjects(u, blockHash, blockId);
		prepareReferencedObjects(u);
	}
	
	
	public int getLastBlockId() {
		OpBlock o = getLastBlock();
		return o != null ? o.getBlockId() : -1;
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
	

	private void prepareReferencedObjects(OpOperation u) {
		Map<String, List<String>> refs = u.getRef();
		Iterator<Entry<String, List<String>>> it = refs.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, List<String>> e = it.next();
			String refName = e.getKey();
			List<String> refObjName = e.getValue();
			OpObject oi = null;
			if(refObjName.size() > 1) {
				// type is necessary
				OpBlockChain blc = this;
				while (blc != null && oi == null) {
					oi = blc.getObjectByName(refObjName);
					blc = blc.parent;
				}
			}
			if(oi == null) {
				error(ErrorType.REF_OBJ_NOT_FOUND, refObjName, u.getHash());
			}
			
			refObjsCache.put(refName, oi);
		}
	}
	
	private ObjectInfo getObjectsByType(String type, boolean create) {
		ObjectInfo oi = objByName.get(type);
		if(oi == null) {
			ObjectInfo pi = parent == null ? null : parent.getObjectsByType(type, false);
			if(create) {
				oi = new ObjectInfo(type, this, pi);
			} else {
				oi = pi;
			}
		}
		return oi;
	}
	
	private OpObject getObjectByName(List<String> o) {
		String objType = o.get(0);
		ObjectInfo ot = getObjectsByType(objType, false);
		if(ot == null) {
			return null;
		}
		return ot.getObjectByFullName(o);
	}
	
	private void prepareDeletedObjects(OpOperation u, String blockHash, int blockId) {
		List<String> deletedRefs = u.getOld();
		deletedObjsCache.clear();
		deletedObjInfoCache.clear();
		
		for(int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = getHashFromAbsRef(delRef);
			int delInd = getIndexFromAbsRef(delRef);
			
			OperationInfo opInfo = getOperationInfo(delHash);
			if(opInfo == null || opInfo.getOp().getNew().size() <= delInd) {
				error(ErrorType.DEL_OBJ_NOT_FOUND, delRef, u.getHash());
			}
			List<OpObject> nw = opInfo.getOp().getNew();
			deletedObjsCache.add(nw.get(delInd));
			deletedObjInfoCache.add(opInfo);
			Iterator<DeleteInfo> delIt = opInfo.getDelInfoIterator(delInd);
			while(delIt.hasNext()) {
				DeleteInfo entry = delIt.next();
				boolean overlaps = false;
				if(blockId == entry.blockId) {
					overlaps = blockHash.equals(entry.blockHash);
				} else if(blockId < entry.blockId) {
					OpBlock bls = getBlockById(entry.blockId);
					if(bls == null || bls.getHash().equals(entry.blockHash)) {
						overlaps = true;
					}
				}
				if(overlaps) {
					error(ErrorType.DEL_OBJ_DOUBLE_DELETED, delRef, u.getHash(), entry.blockHash);
				}
			}
			
			
		}
	}


	private OperationInfo getOperationInfo(String hash) {
		OperationInfo opInfo = null;
		OpBlockChain blc = this;
		while (blc != null && opInfo == null) {
			opInfo = blc.opsByHash.get(hash);
			blc = blc.parent;
		}
		return opInfo;
	}

	
	
}
