package org.openplacereviews.opendb.service;

import java.util.Collection;
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
	 * @return bot class
	 */
	public String getAPI();

	/**
	 * @return history of bot runs
	 */
	Collection getHistoryRuns();

}
