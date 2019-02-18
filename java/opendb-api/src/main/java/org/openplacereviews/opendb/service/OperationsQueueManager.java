package org.openplacereviews.opendb.service;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OperationsQueueManager {

	ConcurrentLinkedQueue<OpDefinitionBean> operationsQueue = new ConcurrentLinkedQueue<OpDefinitionBean>();
	
    @Autowired
    private UsersAndRolesRegistry usersRegistry;

	
	public synchronized void addOperations(List<OpDefinitionBean> operations) {
		for(OpDefinitionBean o : operations) {
			addOperation(o);
		}
	}
	
	public synchronized void addOperation(OpDefinitionBean op) {
		usersRegistry.getQueueUsers().addAuthOperation(op);
		operationsQueue.add(op);
	}
	
	public synchronized void removeSuccessfulOps(OpBlock block) {
		Set<String> hashes = new TreeSet<>();
		for(OpDefinitionBean o : block.getOperations()) {
			hashes.add(o.getHash());
		}
		Iterator<OpDefinitionBean> it = operationsQueue.iterator();
		while(it.hasNext()) {
			OpDefinitionBean o = it.next();
			if(hashes.contains(o.getHash())) {
				it.remove();
				usersRegistry.getQueueUsers().removeAuthOperation(o.getName(), o, false);
			}
		}
	}
	
	public ConcurrentLinkedQueue<OpDefinitionBean> getOperationsQueue() {
		return operationsQueue;
	}

	public synchronized void clearOperations() {
		operationsQueue.clear();
		usersRegistry.getQueueUsers().clear();
	}

	public void init() {
		// TODO Auto-generated method stub
		
	}

	

}
