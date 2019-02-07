package org.openplacereviews.opendb.service;

import java.util.ArrayList;
import java.util.List;

import org.openplacereviews.opendb.ops.IOpenDBOperation;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OperationsRegistry;
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
	public BlocksFormatting formatting;
	
	@Autowired
	public JdbcTemplate jdbcTemplate;
	
	public List<OpBlock> blocks = new ArrayList<OpBlock>(); 

	public synchronized String createBlock() {
		OpBlock bl = new OpBlock();
		while(!queue.getOperationsQueue().isEmpty()) {
			
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
				// should be informed that operation is not valid
				logSystem.operationFailed(op);
			}
		}
		for(IOpenDBOperation o : operations) {
			errorMessage.setLength(0); // to be used later
			o.execute(jdbcTemplate, errorMessage);
		}
		// serialize and confirm block execution
		return formatting.toJson(block);
	}
	


	
}
