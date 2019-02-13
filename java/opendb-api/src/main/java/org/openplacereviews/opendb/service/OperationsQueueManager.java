package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OperationsQueueManager {

	ConcurrentLinkedQueue<OpDefinitionBean> operationsQueue = new ConcurrentLinkedQueue<OpDefinitionBean>();
	
    @Autowired
    private OpenDBUsersRegistry validation;

	
	public synchronized void addOperations(List<OpDefinitionBean> operations) {
		operationsQueue.addAll(operations);
	}
	
	public synchronized void addOperation(OpDefinitionBean op) {
		validation.getQueueUsers().addAuthOperation(op);
		operationsQueue.add(op);
	}
	
	public ConcurrentLinkedQueue<OpDefinitionBean> getOperationsQueue() {
		return operationsQueue;
	}

	public synchronized void clearOperations() {
		operationsQueue.clear();
		validation.getQueueUsers().clear();
	}

	public void init() {
		// TODO Auto-generated method stub
		
	}

}
