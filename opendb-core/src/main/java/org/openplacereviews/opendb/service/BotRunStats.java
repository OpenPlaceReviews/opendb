package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.ops.OpOperation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class BotRunStats {

	public enum FinishStatus {
		FAILED, SUCCESS, INTERRUPTED
	}

	private static final int logSize = 10;
	Deque<BotStats> botStats = new ArrayDeque<>();

	public void createNewState() {
		if (botStats.size() >= logSize) {
			botStats.removeFirst();
		}
		botStats.addLast(new BotStats());
		getCurrentBotState().setRunning();
	}

	public BotStats getCurrentBotState() {
		return botStats.getLast();
	}

	public class BotStats {
		boolean interrupted;
		boolean running;
		List<LogEntry> logEntries;
		Long timeStarted, timeFinished;
		FinishStatus finishStatus;
		int amountOfTasks;
		List<OperationInfo> addedOperations;

		public BotStats() {
			timeStarted = getCurrentTime();
		}

		public String addLogEntry(String msg, Exception e) {
			if (logEntries == null) {
				logEntries = new LinkedList<>();
			}
			if (e != null) {
				logEntries.add(new LogEntry(msg, e));
			} else {
				logEntries.add(new LogEntry(msg, null));
			}
			return msg;
		}

		public void setInterrupted() {
			interrupted = true;
			running = false;
			timeFinished = getCurrentTime();
			finishStatus = FinishStatus.INTERRUPTED;
		}

		public void setRunning() {
			running = true;
		}

		private void saveState(FinishStatus finishStatus, int amountOfTasks) {
			running = false;
			timeFinished = getCurrentTime();
			this.finishStatus = finishStatus;
			this.amountOfTasks = amountOfTasks;
		}

		public void setAmountOfTasks(int amountOfTasks) {
			if (running) {
				this.amountOfTasks = amountOfTasks;
			}
		}

		public void setSuccess(int amountOfTasks) {
			saveState(FinishStatus.SUCCESS, amountOfTasks);
		}

		public void setFailed(int amountOfTasks) {
			saveState(FinishStatus.FAILED, amountOfTasks);
		}

		public boolean addOperation(OpOperation opOperation) {
			if (addedOperations == null) {
				addedOperations = new ArrayList<>();
			}
			return addedOperations.add(new OperationInfo(opOperation));
		}
	}

	class LogEntry {
		public String msg;
		public Exception exception;
		public Long date;

		public LogEntry(String msg, Exception e) {
			this.msg = msg;
			exception = e;
			date = getCurrentTime();
		}

		public String getException() {
			StringWriter errors = new StringWriter();
			exception.printStackTrace(new PrintWriter(errors));
			return String.valueOf(errors);
		}
	}

	class OperationInfo {
		public String hash;
		public Integer added;
		public Integer edited;
		public Integer deleted;

		public OperationInfo(OpOperation opOperation) {
			this.hash = opOperation.getRawHash();
			this.added = opOperation.getCreated().size();
			this.edited = opOperation.getEdited().size();
			this.deleted = opOperation.getDeleted().size();
		}
	}

	private Long getCurrentTime() {
		return new Date().getTime();
	}
}