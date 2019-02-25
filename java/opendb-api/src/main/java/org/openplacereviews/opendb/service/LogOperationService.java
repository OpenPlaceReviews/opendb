package org.openplacereviews.opendb.service;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.springframework.stereotype.Service;

@Service
public class LogOperationService {
	protected static final Log LOGGER = LogFactory.getLog(LogOperationService.class);
	
	ConcurrentLinkedQueue<LogEntry> log  = new ConcurrentLinkedQueue<LogOperationService.LogEntry>();
	
	public enum OperationStatus {
		FAILED_PREPARE(false),
		FAILED_EXECUTE(false),
		FAILED_DEPENDENCIES(false),
		FAILED_VALIDATE(false),
		EXECUTED(true);
		
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
	
	public void init(MetadataDb metadataDB) {
		// no persistence for now
	}
	
	public void logOperation(OperationStatus status, OpOperation op, String message, boolean exceptionOnFail,
			Exception cause) throws OperationFailException {
		LogEntry le = new LogEntry(cause, status, message);
		le.operation = op;
		addLogEntry(exceptionOnFail, le);
	}
	
	public void logOperation(OperationStatus status, OpOperation op, String message, boolean exceptionOnFail) throws OperationFailException {
		logOperation(status, op, message, exceptionOnFail, null);
	}
	
	public void logOperation(OperationStatus status, OpOperation op, String message, Exception cause) {
		logOperation(status, op, message, false, cause);
	}
	
	public void logOperation(OperationStatus status, OpOperation op, String message) {
		logOperation(status, op, message, false, null);
	}
	
	
	public void logBlock(OperationStatus status, OpBlock op, String message, boolean exceptionOnFail) {
		logBlock(status, op, message, exceptionOnFail, null);
	}
	
	public void logBlock(OperationStatus status, OpBlock op, String message, boolean exceptionOnFail, 
			Exception cause) {
		LogEntry le = new LogEntry(cause, status, message);
		le.block = new OpBlock(op);
		addLogEntry(exceptionOnFail, le);
	}

	private void addLogEntry(boolean exceptionOnFail, LogEntry le) {
		log.add(le);
		if(le.status.isSuccessful()) {
			LOGGER.info("SUCCESS: " + getMessage(le));
		} else {
			LOGGER.warn("FAILURE: " + getMessage(le), le.cause);
		}
		if(exceptionOnFail && !le.status.isSuccessful()) {
			throw new OperationFailException(le);
		}
	}

	private String getMessage(LogEntry le) {
		String msg = le.message;
		if(le.operation != null) {
			msg += String.format(", operation: %s, %s, %s", le.operation.getOperationType(), 
					le.operation.getName(), le.operation.getHash()); 
		}
		if(le.block != null) {
			msg += String.format(", block: %s, %s, %s", le.block.getBlockId() +"", 
					le.block.getDateString(), le.block.getHash());
		}
		return msg;
	}
	
	public void clearLogs() {
		log.clear();
	}

	public static class OperationFailException extends RuntimeException {
		private static final long serialVersionUID = 5522972559998855977L;
		private LogEntry logEntry;

		public OperationFailException(LogEntry l) {
			super(l.message, l.cause);
			this.logEntry = l;
		}
		
		public LogEntry getLogEntry() {
			return logEntry;
		}
		
	}
	
	public static class LogEntry {
		OpOperation operation;
		OpBlock block;
		String message;
		OperationStatus status;
		Exception cause;
		long utcTime;
		
		public LogEntry(Exception cause, OperationStatus status, String message) {
			this.utcTime = System.currentTimeMillis();
			this.cause = cause;
			this.message = message;
			this.status = status;
		}
	}

}
