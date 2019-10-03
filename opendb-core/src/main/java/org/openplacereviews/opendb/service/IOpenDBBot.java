package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.ops.OpOperation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.Callable;


public interface IOpenDBBot<T> extends Callable<T> {

	/**
	 * @return null or task description
	 */
	public String getTaskDescription();
	
	/**
	 * @return short task name
	 */
	public String getTaskName();
	
	/**
	 * @return total task counts, -1 if undefined
	 */
	public int taskCount();
	
	/**
	 * @return total progress to be done (any positive number), -1 if undefined
	 */
	public int total();
	
	/**
	 * @return current progress - should be number between [0, total[, -1 if undefined
	 */
	public int progress();
	
	/**
	 * interrupt execution
	 */
	public boolean interrupt();

	/**
	 * @Return current status for bot
	 */
	public boolean isRunning();

	/**
	 * @return history of bot runs
	 */
	public Deque<BotRunStats.BotStats> getHistoryRuns();
	
	
	public String getAPI();


	public static class BotRunStats {

		private static final int logSize = 10;
		Deque<BotRunStats.BotStats> botStats = new ArrayDeque<>();

		public void createNewState() {
			if (botStats.size() > logSize) {
				botStats.removeFirst();
			}
			botStats.addLast(new BotRunStats.BotStats());
			getCurrentBotState().setRunning();
		}

		public BotRunStats.BotStats getCurrentBotState() {
			return botStats.getLast();
		}

		public static class BotStats {
			public enum FinishStatus {
				FAILED, SUCCESS, INTERRUPTED
			}

			boolean interrupted;
			boolean running;
			List<BotStats.LogEntry> logEntries;
			Long timeStarted, timeFinished;
			BotStats.FinishStatus finishStatus;
			int amountOfTasks;
			List<String> addedOperations;

			public BotStats() {
				timeStarted = getCurrentTime();
			}

			public String addLogEntry(String msg, Throwable e) {
				if (logEntries == null) {
					logEntries = new LinkedList<>();
				}
				if (e != null) {
					StringWriter errors = new StringWriter();
					e.printStackTrace(new PrintWriter(errors));
					logEntries.add(new BotStats.LogEntry<>(msg, errors.toString()));
				} else {
					logEntries.add(new BotStats.LogEntry<>(msg, null));
				}
				return msg;
			}

			public void setInterrupted() {
				interrupted = true;
				running = false;
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
				return addedOperations.add(opOperation.getRawHash());
			}

			private Long getCurrentTime() {
				return new Date().getTime();
			}


			final class LogEntry<K, V> implements Map.Entry<K, V> {
				private final K msg;
				private V exception;
				private Long date;

				public LogEntry(K key, V value) {
					this.msg = key;
					this.exception = value;
					this.date = getCurrentTime();
				}

				@Override
				public K getKey() {
					return msg;
				}

				@Override
				public V getValue() {
					return exception;
				}

				public Long getDate() {
					return date;
				}

				@Override
				public V setValue(V value) {
					V old = this.exception;
					this.exception = value;
					return old;
				}
			}
		}
	}
}
