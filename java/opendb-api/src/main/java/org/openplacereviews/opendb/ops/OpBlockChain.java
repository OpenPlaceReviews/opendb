package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.openplacereviews.opendb.OUtils;

public class OpBlockChain {
	
	private final OpBlockChain parent;
	private final String parentLastBlockHash;
	
	private Deque<OpBlock> blocks = new ConcurrentLinkedDeque<OpBlock>();
	private Map<String, Integer> blockDepth = new ConcurrentHashMap<>();
	private Map<String, OperationInfo> opsByHash = new ConcurrentHashMap<>();
	private Map<String, ObjectInfo> objByName = new ConcurrentHashMap<>();
	private boolean immutable;

	// TODO write tests for isParentChainOrOverlaps
	private static enum ErrorType {
		PREV_BLOCK_HASH("Previous block hash is not equal '%s' != '%s': block '%s'"),
		PREV_BLOCK_ID("Previous block id is not equal '%d' != '%d': block '%s'"),
		BLOCK_HASH_IS_DUPLICATED("Block hash is duplicated '%s' in block '%d' and '%d'"),
		OP_HASH_IS_DUPLICATED("Operation hash is duplicated '%s' in block '%d' and '%s'"),
		DEL_OBJ_NOT_FOUND("Object to delete wasn't found '%s': op '%s'"),
		REF_OBJ_NOT_FOUND("Object to reference wasn't found '%s': op '%s'"),
		DEL_OBJ_DOUBLE_DELETED("Object '%s' was double delete in '%s' and'%s'");
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
		OpBlock lastBlock = null;
		if(this.parent != null) {
			this.parent.makeImmutable();
			lastBlock = this.parent.getLastBlock();
		}
		this.parentLastBlockHash = lastBlock == null ? "" : lastBlock.getHash();
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
	
	public boolean isParentChainOrOverlaps(OpBlock currentLastBlock, OpBlockChain value) {
		// quick checks
		OpBlockChain siterator = this;
		while(siterator != null && siterator != value && siterator != value.parent) {
			siterator = siterator.parent;
		}
		if(siterator == value) {
			return true;
		}
		String lastParent = value.parentLastBlockHash;
		if(lastParent.equals(currentLastBlock.getHash())) {
			// it is strange impossible situation cause in that case this block chain should be immutable
			// so return true to fail some validation
			return true;
		}
		// not very efficient method
		Set<String> allBlocks = new TreeSet<String>();
		for(OpBlock blc : value.blocks) {
			allBlocks.add(blc.getHash());
		}
		siterator = this;
		boolean overlaps = allBlocks.contains(currentLastBlock.getHash());
		while (!overlaps && siterator != null) {
			for (OpBlock o : this.blocks) {
				String blHash = o.getHash();
				if (allBlocks.contains(blHash)) {
					overlaps = true;
					break;
				} else if (lastParent.equals(blHash)) {
					// in this situation blockchain share lastParent leaf but the previous is not part of another blockchain
					overlaps = false;
					siterator = null;
					break;
				}
			}
			if (siterator != null) {
				siterator = siterator.parent;
			}
		}
		return overlaps;
	}

	public synchronized void addBlock(OpBlock block) {
		if(immutable) {
			throw new IllegalStateException("Object is immutable");
		}
		OpBlock prevBlock = getLastBlock();
		int pid = -1;
		int cid = block.getBlockId();
		if(prevBlock != null) {
			if(!OUtils.equals(prevBlock.getHash(), block.getStringValue(OpBlock.F_PREV_BLOCK_HASH))) {
				error(ErrorType.PREV_BLOCK_HASH, prevBlock.getHash(), 
						block.getStringValue(OpBlock.F_PREV_BLOCK_HASH), block.getHash());
			}
			pid = prevBlock.getBlockId();
		}
		if(pid != cid) {
			error(ErrorType.PREV_BLOCK_ID, pid, block.getBlockId(), block.getHash());
		}
		int dupBl = getBlockDepth(block.getHash());
		if(dupBl != -1) {
			error(ErrorType.BLOCK_HASH_IS_DUPLICATED, block.getHash(), block.getBlockId(), dupBl);
		}
		
		List<OpOperation> ops = block.getOperations();
		Map<String, OpObject> refObjs = new HashMap<String, OpObject>();
		List<OpObject> deletedObjs = new ArrayList<OpObject>();
		for(OpOperation u : ops) {
			
			OperationInfo oin = getOperationInfo(u.getHash());
			if(oin != null) {
				error(ErrorType.OP_HASH_IS_DUPLICATED, u.getHash(), oin.getBlockId(), block.getHash());
			}
			OperationInfo info = new OperationInfo(u, this, cid);
			processDeletedObjects(block, deletedObjs, u);
			processReferencedObjects(block, refObjs, u);
			for(OpObject newObj : u.getNew()){
				List<String> id = newObj.getId();
				if(id != null && id.size() > 0) {
					String objType = id.get(0);
					ObjectInfo oinf = getObjectsByType(objType, true);
					oinf.add(id, newObj);
				}
			}
			opsByHash.put(u.getHash(), info);
		}
		blockDepth.put(block.getHash(), block.getBlockId());
		blocks.push(block);
	}
	
	
	public int getLastBlockId() {
		OpBlock o = getLastBlock();
		return o != null ? o.getBlockId() : -1;
	}
	
	public int getSize() {
		return blocks.size();
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
	

	private void processReferencedObjects(OpBlock block, Map<String, OpObject> refObjs, OpOperation u) {
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
			
			refObjs.put(refName, oi);
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
	
	private void processDeletedObjects(OpBlock block, List<OpObject> deletedObjs, OpOperation u) {
		List<String> deletedRefs = u.getOld();
		deletedObjs.clear();
		
		for(int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = getHashFromAbsRef(delRef);
			int delInd = getIndexFromAbsRef(delRef);
			
			OperationInfo opInfo = getOperationInfo(delHash);
			if(opInfo == null || opInfo.getOp().getNew().size() <= delInd) {
				error(ErrorType.DEL_OBJ_NOT_FOUND, delRef, u.getHash());
			}
			List<OpObject> nw = opInfo.getOp().getNew();
			deletedObjs.add(nw.get(delInd));
			Iterator<Entry<OpOperation, OpBlockChain>> delIt = opInfo.getDelInfoIterator(delInd);
			while(delIt.hasNext()) {
				Entry<OpOperation, OpBlockChain> entry = delIt.next();
				if(isParentChainOrOverlaps(block, entry.getValue())) {
					error(ErrorType.DEL_OBJ_DOUBLE_DELETED, 
							delRef, u.getHash(), entry.getKey().getHash());
				}
			}
			
			opInfo.addDelInfo(delInd, u, this);
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
