package org.openplacereviews.opendb.ops;

import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.ops.de.OperationDeleteInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

class OpPrivateOperations {
	// operations to be stored like a queue
	private final Deque<OpOperation> queueOperations = new ConcurrentLinkedDeque<OpOperation>();
	// stores information about created and deleted objects in this blockchain 
	private final Map<String, OperationDeleteInfo> opsByHash = new ConcurrentHashMap<>();
	private final BlockDbAccessInterface dbAccess;
	
	public OpPrivateOperations(BlockDbAccessInterface dbAccess) {
		this.dbAccess = dbAccess;
	}

	public Deque<OpOperation> getQueueOperations() {
		if(dbAccess != null) {
			// in that case it could just return  empty list
			// throw new UnsupportedOperationException("Queue is not supported by db access");
		}
		return queueOperations;
	}

	public boolean isQueueEmpty() {
//		if(dbAccess != null) {
			// in that case it could just return  empty
			// return true;
//		}
		return queueOperations.isEmpty();
	}

	public OperationDeleteInfo getOperationInfo(String rawHash) {
		if(dbAccess != null) {
			return dbAccess.getOperationInfo(rawHash);
		}
		return opsByHash.get(rawHash);
	}

	public Collection<OperationDeleteInfo> getOperationInfos() {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		return opsByHash.values();
	}
	

	void clearQueueOperations(boolean deleteInfo) {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		queueOperations.clear();
		if(deleteInfo) {
			opsByHash.clear();
		}
	}
	
	void addNewOperation(OpOperation u) {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		OperationDeleteInfo infop = new OperationDeleteInfo();
		infop.op = u;
		infop.create = true;
		opsByHash.put(u.getRawHash(), infop);
		queueOperations.add(u);		
	}
	
	OperationDeleteInfo addDeletedObject(String delHash, int delInd, OpOperation opRef) {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		OperationDeleteInfo pi = opsByHash.get(delHash);
		if(pi.deletedObjects == null) {
			pi.deletedObjects = new boolean[pi.op.getNew().size()];
		}
		if(pi.deletedOpHashes == null) {
			pi.deletedOpHashes = new ArrayList<String>();
		} else { 
			pi.deletedOpHashes = new ArrayList<String>(pi.deletedOpHashes);
		}
		pi.deletedOpHashes.add(opRef.getRawHash());
		pi.deletedObjects[delInd] = true;
		return pi;
	}
	
	void copyAndMerge(OpPrivateOperations copy, OpPrivateOperations parent) {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		TreeSet<String> setOfOperations = new TreeSet<String>(parent.opsByHash.keySet());
		setOfOperations.addAll(copy.opsByHash.keySet());
		for (String operation : setOfOperations) {
			OperationDeleteInfo cp = copy.opsByHash.get(operation);
			OperationDeleteInfo pp = parent.opsByHash.get(operation);
			this.opsByHash.put(operation, mergeDeleteInfo(cp, pp));
		}		
	}
	
	void removeOperationInfo(OpOperation op) {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		// delete operation itself
		OperationDeleteInfo odi = opsByHash.remove(op.getRawHash());
		odi.create = false;
		// delete deleted objects by name
		List<String> deletedRefs = op.getOld();
		String rawHash = op.getRawHash();
		for (int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = OpBlockChain.getHashFromAbsRef(delRef);
			int delInd = OpBlockChain.getIndexFromAbsRef(delRef);
			OperationDeleteInfo pi = opsByHash.get(delHash);
			if (pi != null && pi.deletedObjects.length > delInd) {
				pi.deletedObjects[delInd] = false;
			}
			if(pi != null && pi.deletedOpHashes != null) {
				pi.deletedOpHashes.remove(rawHash);
			}
		}
	}
	
	void removeOperations(Set<String> operationsSet) {
		if(dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		Iterator<OpOperation> it = queueOperations.iterator();
		while(it.hasNext()) {
			OpOperation o = it.next();
			if(operationsSet.contains(o.getRawHash())) {
				it.remove();
				removeOperationInfo(o);
			}
		}		
	}
	
	static OperationDeleteInfo mergeDeleteInfo(OperationDeleteInfo cdi, OperationDeleteInfo pdi) {
		OperationDeleteInfo ndi = new OperationDeleteInfo();
		ndi.op = pdi != null ? pdi.op : cdi.op;
		ndi.create = (pdi != null && pdi.create) || (cdi != null && cdi.create);
		if((pdi != null && pdi.create) && (cdi != null && cdi.create)) {
			// assert
			throw new IllegalArgumentException("Operation was created twice");
		}
		int psz = (pdi == null || pdi.deletedObjects == null) ? 0 : pdi.deletedObjects.length;
		int sz = (cdi == null || cdi.deletedObjects == null) ? 0 : cdi.deletedObjects.length;
		int length = Math.max(sz, psz);
		ndi.deletedObjects = new boolean[length];
		ndi.deletedOpHashes = new ArrayList<String>();
		if(pdi != null && pdi.deletedOpHashes != null) {
			ndi.deletedOpHashes.addAll(pdi.deletedOpHashes);
		}
		if(cdi != null && cdi.deletedOpHashes != null) {
			ndi.deletedOpHashes.addAll(cdi.deletedOpHashes);
		}
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

	

	
}
