package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.stereotype.Service;

@Service
public class OperationsQueue {

	ConcurrentLinkedQueue<OpDefinitionBean> operationsQueue = new ConcurrentLinkedQueue<OpDefinitionBean>();
	
	public void addOperations(List<OpDefinitionBean> operations) {
		operationsQueue.addAll(operations);
	}
	
	public void addOperation(OpDefinitionBean op) {
		operationsQueue.add(op);
	}
	
	public ConcurrentLinkedQueue<OpDefinitionBean> getOperationsQueue() {
		return operationsQueue;
	}

	public void clearOperations() {
		operationsQueue.clear();
	}

}
