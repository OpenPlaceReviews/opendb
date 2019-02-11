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
		public LogEntry(OpDefinitionBean op, String message) {
			this.op = op;
			this.message = message;
		}
	}
	
	public ConcurrentLinkedQueue<LogEntry> getLog() {
		return log;
	}
	
	public void operationDiscarded(OpDefinitionBean op, String message) {
		LogEntry le = new LogEntry(op, message);
		log.add(le);
	}
	
	public void clearLogs() {
		log.clear();
	}

}
