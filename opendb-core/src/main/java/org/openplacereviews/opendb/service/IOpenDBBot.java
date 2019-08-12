package org.openplacereviews.opendb.service;

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
	 * @return total progress to be done, -1 if undefined
	 */
	public int total();
	
	/**
	 * @return current progress, -1 if undefined
	 */
	public int progress();
}
