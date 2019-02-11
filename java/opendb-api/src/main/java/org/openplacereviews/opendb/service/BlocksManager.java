package org.openplacereviews.opendb.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.ops.IOpenDBOperation;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.openplacereviews.opendb.service.OpenDBUsersRegistry.ActiveUsersContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class BlocksManager {

	@Autowired
	public OperationsQueue queue;
	
	@Autowired
	public OperationsRegistry registry;
	
	@Autowired
	public LogOperationService logSystem;
	
	@Autowired
	public OpenDBUsersRegistry usersRegistry;
	
	@Autowired
	public JdbcTemplate jdbcTemplate;
	
	public static final int MAX_BLOCK_SIZE = 1000;
	
	public static final int MAX_BLOCK_SIZE_MB = 1 << 20;
	
	public List<OpBlock> blocks = new ArrayList<OpBlock>(); 

	public synchronized String createBlock() {
		OpBlock bl = new OpBlock();
		List<OpDefinitionBean> candidates = bl.getOperations();
		ConcurrentLinkedQueue<OpDefinitionBean> q = queue.getOperationsQueue();
		int size = 0;
		ActiveUsersContext au = new OpenDBUsersRegistry.ActiveUsersContext(usersRegistry.getBlockUsers());
		while(!q.isEmpty()) {
			OpDefinitionBean o = q.poll();
			int l = usersRegistry.toJson(o).length();
			String validMsg = null; 
			try {
				if(!usersRegistry.validateSignatures(au, o)) {
					validMsg = "not verified";
				}
			} catch (Exception e) {
				validMsg = e.getMessage();
			}
			if(validMsg != null) {
				logSystem.operationDiscarded(o, String.format("Failed to verify operation signature: %s", validMsg));
				continue;
			}
			
			if(l > MAX_BLOCK_SIZE_MB / 2) {
				logSystem.operationDiscarded(o, String.format("Operation discarded due to size limit %d", l));
			}
			if(size + l > MAX_BLOCK_SIZE) {
				break;
			}
			if(candidates.size() >= MAX_BLOCK_SIZE) {
				break;
			}
			au.addAuthOperation(o);
			candidates.add(o);
		}
		return executeBlock(bl);
	}

	
	public synchronized String replicateBlock(OpBlock remoteBlock) {
		return executeBlock(remoteBlock);
	}


	private String executeBlock(OpBlock block) {
		List<IOpenDBOperation> operations = new ArrayList<IOpenDBOperation>();
		StringBuilder errorMessage = new StringBuilder();
		for(OpDefinitionBean def : block.getOperations()) {
			IOpenDBOperation op = registry.createOperation(def);
			errorMessage.setLength(0); // to be used later
			boolean valid = op != null && op.prepare(def, errorMessage);
			if(valid) { 
				operations.add(op);
			} else {
				logSystem.operationDiscarded(def,
						String.format("Operations couldn't be prepared for execution: %s", errorMessage.toString()));
			}
		}
		for(IOpenDBOperation o : operations) {
			errorMessage.setLength(0); // to be used later
			o.execute(jdbcTemplate, errorMessage);
		}
		// serialize and confirm block execution
		// save & create block 
		// later we shouldn't keep blocks in memory
		blocks.add(block);
		return usersRegistry.toJson(block);
	}
	


	
}
