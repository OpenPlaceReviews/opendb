package org.openplacereviews.opendb.service;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.DBConstants;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OperationsQueueManager {

	protected static final Log LOGGER = LogFactory.getLog(OperationsQueueManager.class);
	ConcurrentLinkedQueue<OpOperation> operationsQueue = new ConcurrentLinkedQueue<OpOperation>();
	
    @Autowired
    private UsersAndRolesRegistry usersRegistry;
    
    @Autowired
    private DBDataManager dbManager;

    
    public void init(MetadataDb metadataDB) {
		LOGGER.info("... Operations queue. Loading operations queue ...");
		if(metadataDB.tablesSpec.containsKey(DBConstants.QUEUE_TABLE)) {
			List<OpOperation> ops = dbManager.loadOperations(DBConstants.QUEUE_TABLE);
			addOperations(ops);
		}
		
		LOGGER.info(String.format("+++ Operations queue is inititialized. Loaded %d operations.", operationsQueue.size()));		
	}
    
	
	public synchronized void addOperations(List<OpOperation> operations) {
		for(OpOperation o : operations) {
			addOperation(o);
		}
	}
	
	public synchronized void addOperation(OpOperation op) {
		usersRegistry.getQueueUsers().addAuthOperation(op);
		operationsQueue.add(op);
	}
	
	public synchronized void removeSuccessfulOps(OpBlock block) {
		Set<String> hashes = new TreeSet<>();
		for(OpOperation o : block.getOperations()) {
			hashes.add(o.getHash());
		}
		Iterator<OpOperation> it = operationsQueue.iterator();
		while(it.hasNext()) {
			OpOperation o = it.next();
			if(hashes.contains(o.getHash())) {
				it.remove();
				usersRegistry.getQueueUsers().removeAuthOperation(o.getName(), o, false);
			}
		}
	}
	
	public ConcurrentLinkedQueue<OpOperation> getOperationsQueue() {
		return operationsQueue;
	}

	public synchronized void clearOperations() {
		operationsQueue.clear();
		usersRegistry.getQueueUsers().clear();
	}

	

}
