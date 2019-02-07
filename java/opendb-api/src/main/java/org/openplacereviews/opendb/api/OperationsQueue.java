package org.openplacereviews.opendb.api;

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
	
	public ConcurrentLinkedQueue<OpDefinitionBean> getOperationsQueue() {
		return operationsQueue;
	}

	public void clearOperations() {
		operationsQueue.clear();
	}

}
