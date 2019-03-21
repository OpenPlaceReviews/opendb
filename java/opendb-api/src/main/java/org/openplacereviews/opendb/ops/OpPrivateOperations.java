package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

class OpPrivateOperations {
	// operations to be stored like a queue
	private final Deque<OpOperation> operations = new ConcurrentLinkedDeque<OpOperation>();
	// stores information about created and deleted objects in this blockchain 
	private final Map<String, OperationDeleteInfo> opsByHash = new ConcurrentHashMap<>();
	
	static class OperationDeleteInfo {
		OpOperation op;
		boolean create;
		boolean[] deletedObjects;
	}

	public Collection<OpOperation> getAllOperations() {
		return operations;
	}

	public boolean isEmpty() {
		return operations.isEmpty();
	}

	public OperationDeleteInfo getOperationInfo(String hash) {
		return opsByHash.get(hash);
	}
	
	

	void clearOnlyOperationsList() {
		operations.clear();
	}
	
	void addNewOperation(OpOperation u) {
		OperationDeleteInfo infop = new OperationDeleteInfo();
		infop.op = u;
		infop.create = true;
		opsByHash.put(u.getRawHash(), infop);
		operations.add(u);		
	}
	
	void addDeletedObject(String delHash, int delInd, OpOperation opRef) {
		OperationDeleteInfo pi = opsByHash.get(delHash);
		if(pi == null) {
			pi = new OperationDeleteInfo();
			pi.op = opRef;
			opsByHash.put(delHash, pi);
		}
		if(pi.deletedObjects == null) {
			pi.deletedObjects = new boolean[opRef.getNew().size()];
		}
		pi.deletedObjects[delInd] = true;
	}
	
	void copyAndMerge(OpPrivateOperations copy, OpPrivateOperations parent) {
		TreeSet<String> setOfOperations = new TreeSet<String>(parent.opsByHash.keySet());
		setOfOperations.addAll(copy.opsByHash.keySet());
		for (String operation : setOfOperations) {
			OperationDeleteInfo cp = copy.opsByHash.get(operation);
			OperationDeleteInfo pp = parent.opsByHash.get(operation);
			this.opsByHash.put(operation, mergeDeleteInfo(cp, pp));
		}		
	}
	
	void removeOperationInfo(OpOperation op) {
		// delete operation itself
		OperationDeleteInfo odi = opsByHash.remove(op.getRawHash());
		odi.create = false;
		// delete deleted objects by name
		List<String> deletedRefs = op.getOld();
		for (int i = 0; i < deletedRefs.size(); i++) {
			String delRef = deletedRefs.get(i);
			String delHash = OpBlockChain.getHashFromAbsRef(delRef);
			int delInd = OpBlockChain.getIndexFromAbsRef(delRef);
			OperationDeleteInfo pi = opsByHash.get(delHash);
			if (pi != null && pi.deletedObjects.length > delInd) {
				pi.deletedObjects[delInd] = false;
			}
		}
	}
	
	void removeOperations(Set<String> operationsSet) {
		Iterator<OpOperation> it = operations.iterator();
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
		ndi.create = (pdi != null && pdi.create) || (cdi != null && cdi.create);
		if((pdi != null && pdi.create) && (cdi != null && cdi.create)) {
			// assert
			throw new IllegalArgumentException("Operation was created twice");
		}
		int psz = (pdi == null || pdi.deletedObjects == null) ? 0 : pdi.deletedObjects.length;
		int sz = (cdi == null || cdi.deletedObjects == null) ? 0 : cdi.deletedObjects.length;
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

	
}
