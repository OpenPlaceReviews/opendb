package org.openplacereviews.opendb.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.openplacereviews.opendb.ops.auth.SignUpOperation;
import org.openplacereviews.opendb.service.LogOperationService.OperationStatus;
import org.openplacereviews.opendb.service.OpenDBUsersRegistry.ActiveUsersContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class BlocksManager {

	private int BLOCK_ID = 1;
	private String PREV_BLOCK_HASH = "";
	
	
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
	
	public synchronized String createBlock() {
		OpBlock bl = new OpBlock();
		List<OpDefinitionBean> candidates = bl.getOperations();
		ConcurrentLinkedQueue<OpDefinitionBean> q = queue.getOperationsQueue();
		pickupOpsFromQueue(candidates, q, false);
		return executeBlock(bl, false);
	}


	
	private Map<String, Set<String>> pickupOpsFromQueue(List<OpDefinitionBean> candidates,
			Queue<OpDefinitionBean> q, boolean exceptionOnFail) {
		int size = 0;
		ActiveUsersContext au = new OpenDBUsersRegistry.ActiveUsersContext(usersRegistry.getBlockUsers());
		Map<String, Set<String>> authTxDependencies = new HashMap<String, Set<String>>();
		while(!q.isEmpty()) {
			OpDefinitionBean o = q.poll();
			int l = usersRegistry.toJson(o).length();
			String validMsg = null; 
			try {
				if(!usersRegistry.validateSignatures(au, o)) {
					validMsg = "not verified";
				}
				if(!usersRegistry.validateHash(o)) {
					validMsg = "hash is not valid";
				}
				if(!usersRegistry.validateSignatureHash(o)) {
					validMsg = "signature hash is not valid";
				}
			} catch (Exception e) {
				validMsg = e.getMessage();
			}
			if(validMsg != null) {
				logSystem.logOperation(OperationStatus.FAILED_PREPARE, o, String.format("Failed to verify operation signature: %s", validMsg),
						exceptionOnFail);
				continue;
			}
			
			if(l > MAX_BLOCK_SIZE_MB / 2) {
				logSystem.logOperation(OperationStatus.FAILED_PREPARE, o, String.format("Operation discarded due to size limit %d", l), exceptionOnFail);
				continue;
			}
			if(size + l > MAX_BLOCK_SIZE) {
				break;
			}
			if(candidates.size() >= MAX_BLOCK_SIZE) {
				break;
			}
			boolean authOp = au.addAuthOperation(o);
			if(authOp) {
				String uname = o.getStringValue(SignUpOperation.F_NAME);
				if(!authTxDependencies.containsKey(uname)) {
					authTxDependencies.put(uname, new LinkedHashSet<String>());
				}
				o.setTransientTxDependencies(new ArrayList<String>(authTxDependencies.get(uname)));
				authTxDependencies.get(uname).add(o.getHash());
			}
			candidates.add(o);
		}
		return authTxDependencies;
	}

	
	public synchronized String replicateBlock(OpBlock remoteBlock) {
		LinkedList<OpDefinitionBean> ops = new LinkedList<>(remoteBlock.getOperations());
		ArrayList<OpDefinitionBean> cand = new ArrayList<>();
		try {
			pickupOpsFromQueue(cand, ops, true);
			if(ops.size() != 0) {
				throw new RuntimeException("The block could not be formed with all transactions");
			}
			return executeBlock(remoteBlock, true);
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			throw e;
		}
	}


	private String executeBlock(OpBlock block, boolean exceptionOnFail) {
		List<OpenDBOperationExec> operations = prepareOperationCtxToExec(block);
		executeOperations(block, operations, exceptionOnFail);
		if(block.blockId == 0) {
			block.date = System.currentTimeMillis();
			block.blockId = BLOCK_ID++;
			block.previousBlockHash = PREV_BLOCK_HASH;
			block.hash = "TODO";
			block.merkleTreeHash = "TODO";
		} else {
			// validate merkle tree hash 
			// validate hash
			// validate prev block hash
		}
		
		PREV_BLOCK_HASH = block.hash;
		// TODO calculate hash, serialize, save db block, save folder block, save db operation?
		// later we shouldn't keep blocks in memory
//		blocks.add(block);
		return usersRegistry.toJson(block);
	}



	private void executeOperations(OpBlock block, List<OpenDBOperationExec> operations, boolean exceptionOnFail) {
		block.getOperations().clear();
		Map<String, OpDefinitionBean> executedTx = new TreeMap<String, OpDefinitionBean>();
		for (OpenDBOperationExec o : operations) {
			boolean allDeps = checkAllDependencies(executedTx, o.getDefinition().getTransientTxDependencies());
			if(allDeps) {
				allDeps = checkAllDependencies(executedTx, o.getDefinition().getStringList(OpDefinitionBean.F_DEPENDENCIES));
			}
			if(!allDeps) {
				logSystem.logOperation(OperationStatus.FAILED_DEPENDENCIES, o.getDefinition(), 
						String.format("Operations has dependencies there were not executed yet", o.getDefinition().getHash()), exceptionOnFail);
				continue;
			}
			if(executedTx.containsKey(o.getDefinition().getHash())) {
				logSystem.logOperation(OperationStatus.FAILED_EXECUTE, o.getDefinition(), 
						String.format("Operations has duplicate hash in same block: %s", o.getDefinition().getHash()), exceptionOnFail);
				continue;
			}
			
			boolean execute = false;
			String err = "";
			try {
				execute = o.execute(jdbcTemplate);
				block.getOperations().add(o.getDefinition());
				executedTx.put(o.getDefinition().getHash(), o.getDefinition());
			} catch (Exception e) {
				err = e.getMessage();
			}
			if (!execute) {
				logSystem.logOperation(OperationStatus.FAILED_EXECUTE,
						o.getDefinition(), String.format("Operations failed to execute: %s", err), exceptionOnFail);
			} else {
				logSystem.logOperation(OperationStatus.EXECUTED, o.getDefinition(), "OK", exceptionOnFail);
			}
		}
	}


	private List<OpenDBOperationExec> prepareOperationCtxToExec(OpBlock block) {
		List<OpenDBOperationExec> operations = new ArrayList<OpenDBOperationExec>();
		for(OpDefinitionBean def : block.getOperations()) {
			OpenDBOperationExec op = registry.createOperation(def);
			boolean valid = false;
			String err = "";
			try {
				valid = op != null && op.prepare(def);
			} catch (Exception e) {
				err = e.getMessage();
			}
			if(valid) { 
				operations.add(op);
			} else {
				logSystem.logOperation(OperationStatus.FAILED_PREPARE, def,
						String.format("Operations couldn't be validated for execution: %s", err));
			}
		}
		return operations;
	}


	private boolean checkAllDependencies(Map<String, OpDefinitionBean> executedTx, List<String> dp) {
		for (String d : dp) {
			if (!executedTx.containsKey(d)) {
				return false;
			}
		}
		return true;
	}
	


	
}
