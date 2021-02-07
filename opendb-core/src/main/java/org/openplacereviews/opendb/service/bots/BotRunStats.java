package org.openplacereviews.opendb.service.bots;

import org.openplacereviews.opendb.ops.OpOperation;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;

public class BotRunStats {

	public Deque<BotStats> botStats = new ArrayDeque<>();

	private static long currentTime() {
		return System.currentTimeMillis();
	}
	
	public boolean isRunning() {
		return botStats.isEmpty() ? false : getCurrentBotState().running;
	}

	public void createNewState(int maxAmountLogs) {
		while (botStats.size() >= maxAmountLogs) {
			botStats.removeFirst();
		}
		botStats.addLast(new BotStats());
		getCurrentBotState().setRunning();
	}

	public BotStats getCurrentBotState() {
		return botStats.getLast();
	}
	public enum FinishStatus {
		FAILED, SUCCESS, INTERRUPTED
	}

	public static class BotStats {
		public boolean running;
		boolean interrupted;
		List<LogEntry> logEntries;
		Long timeStarted, timeFinished;
		FinishStatus finishStatus;
		int amountOfTasks;
		List<OperationInfo> addedOperations;

		public BotStats() {
			timeStarted = currentTime();
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
			timeFinished = currentTime();
			finishStatus = FinishStatus.INTERRUPTED;
		}

		public void setRunning() {
			running = true;
		}

		private void saveState(FinishStatus finishStatus, int amountOfTasks) {
			running = false;
			timeFinished = currentTime();
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

	public static class LogEntry implements Serializable {
		private static final long serialVersionUID = -8226300775770347363L;
		public String msg;
		public String exception;
		public Long date;

		public LogEntry(String msg, Exception e) {
			this.msg = msg;
			if (e != null) {
				StringWriter errors = new StringWriter();
				e.printStackTrace(new PrintWriter(errors));
				exception = errors.toString();
			}
			date = currentTime();
		}
	}

	public static class OperationInfo {
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
}