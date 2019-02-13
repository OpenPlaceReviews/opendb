package org.openplacereviews.opendb.service;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.stereotype.Service;

@Service
public class LogOperationService {
	ConcurrentLinkedQueue<LogEntry> log  = new ConcurrentLinkedQueue<LogOperationService.LogEntry>();

	public static class LogEntry {
		OpDefinitionBean operation;
		OpBlock block;
		String message;
		OperationStatus status;
		public LogEntry(OperationStatus status, String message) {
			this.message = message;
			this.status = status;
		}
	}
	
	public enum OperationStatus {
		FAILED_PREPARE(false),
		FAILED_EXECUTE(false),
		FAILED_DEPENDENCIES(false),
		FAILED_VALIDATE(false),
		EXECUTED(false);
		
		private boolean success;
		private OperationStatus(boolean s) {
			this.success = s;
		}
		
		public boolean isSuccessful() {
			return success;
		}
	}
	
	public ConcurrentLinkedQueue<LogEntry> getLog() {
		return log;
	}
	
	public static class OperationFailException extends RuntimeException {
		private static final long serialVersionUID = 5522972559998855977L;
		private OperationStatus status;
		private Object op;

		public OperationFailException(OperationStatus status, Object op, String message) {
			super(message);
			this.status = status;
			this.op = op;
		}
		
		public OpDefinitionBean getOp() {
			return op instanceof OpDefinitionBean? (OpDefinitionBean) op : null;
		}
		
		public OpBlock getBlock() {
			return op instanceof OpBlock? (OpBlock) op : null;
		}
		
		public OperationStatus getStatus() {
			return status;
		}
		
	}
	
	public void logOperation(OperationStatus status, OpDefinitionBean op, String message, boolean exceptionOnFail) throws OperationFailException {
		LogEntry le = new LogEntry(status, message);
		le.operation = op;
		log.add(le);
		if(exceptionOnFail && !status.isSuccessful()) {
			throw new OperationFailException(status, op, message);
		}
	}
	
	public void logOperation(OperationStatus status, OpDefinitionBean op, String message) {
		logOperation(status, op, message, false);
	}
	
	
	public void logBlock(OperationStatus status, OpBlock op, String message, boolean exceptionOnFail) {
		LogEntry le = new LogEntry(status, message);
		le.block = new OpBlock(op);
		log.add(le);
		if(exceptionOnFail && !status.isSuccessful()) {
			throw new OperationFailException(status, le.block, message);
		}
	}
	
	public void clearLogs() {
		log.clear();
	}

}
