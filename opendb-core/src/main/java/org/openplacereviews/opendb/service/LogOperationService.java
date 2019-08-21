package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ErrorType;
import org.openplacereviews.opendb.ops.OpBlockchainRules.ValidationListener;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.ops.OpObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
public class LogOperationService implements ValidationListener {

	protected static final Log LOGGER = LogFactory.getLog(LogOperationService.class);

	private static final int LIMIT = 1000;
	@Autowired
	private JsonFormatter formatter;
	
	ConcurrentLinkedDeque<LogEntry> log  = new ConcurrentLinkedDeque<LogOperationService.LogEntry>();
	
	public Collection<LogEntry> getLog() {
		return log;
	}
	
	@Override
	public void logError(OpObject o, ErrorType e, String msg, Exception cause) {
		LogEntry le = new LogEntry(cause, e, msg);
		le.obj = o;
		addLogEntry(le);		
	}
	
	public void logSuccessBlock(OpBlock op, String message) {
		logError(op, null, message, null);
	}
	

	private void addLogEntry(LogEntry le) {
		while(log.size() > LIMIT) {
			log.removeLast();
		}
		log.add(le);
		if(le.status == null) {
			LOGGER.info("SUCCESS: " + getMessage(le));
		} else {
			LOGGER.warn("FAILURE: " + getMessage(le), le.cause);
			if(le.obj != null) {
				LOGGER.info("FAILURE OBJECT: " + formatter.objToJson(le.obj));
			}
		}
	}

	private String getMessage(LogEntry le) {
		String msg = le.message;
//		if(le.operation != null) {
//			msg += String.format(", operation: %s, %s, %s", le.operation.getOperationType(), 
//					le.operation.getName(), le.operation.getHash()); 
//		}
//		if(le.block != null) {
//			msg += String.format(", block: %s, %s, %s", le.block.getBlockId() +"", 
//					le.block.getDateString(), le.block.getHash());
//		}
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
		OpObject obj;
		String message;
		ErrorType status;
		Exception cause;
		long utcTime;
		
		public LogEntry(Exception cause, ErrorType status, String message) {
			this.utcTime = System.currentTimeMillis();
			this.cause = cause;
			this.message = message;
			this.status = status;
		}
	}

	

}
