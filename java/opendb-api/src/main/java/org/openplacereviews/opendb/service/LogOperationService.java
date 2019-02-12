package org.openplacereviews.opendb.service;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.stereotype.Service;

@Service
public class LogOperationService {
	ConcurrentLinkedQueue<LogEntry> log  = new ConcurrentLinkedQueue<LogOperationService.LogEntry>();

	public static class LogEntry {
		OpDefinitionBean op;
		String message;
		OperationStatus status;
		public LogEntry(OperationStatus status, OpDefinitionBean op, String message) {
			this.op = op;
			this.message = message;
			this.status = status;
		}
	}
	
	public enum OperationStatus {
		FAILED_PREPARE(false),
		FAILED_EXECUTE(false),
		FAILED_DEPENDENCIES(false),
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
		private OpDefinitionBean op;

		public OperationFailException(OperationStatus status, OpDefinitionBean op, String message) {
			super(message);
			this.status = status;
			this.op = op;
		}
		
		public OpDefinitionBean getOp() {
			return op;
		}
		
		public OperationStatus getStatus() {
			return status;
		}
		
	}
	
	public void logOperation(OperationStatus status, OpDefinitionBean op, String message, boolean exceptionOnFail) throws OperationFailException {
		LogEntry le = new LogEntry(status, op, message);
		log.add(le);
		if(exceptionOnFail && !status.isSuccessful()) {
			throw new OperationFailException(status, op, message);
		}
	}
	
	public void logOperation(OperationStatus status, OpDefinitionBean op, String message) {
		logOperation(status, op, message, false);
	}
	
	public void clearLogs() {
		log.clear();
	}

}
