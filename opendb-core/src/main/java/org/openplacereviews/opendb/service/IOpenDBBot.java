package org.openplacereviews.opendb.service;

import java.util.concurrent.Callable;

public interface IOpenDBBot<T> extends Callable<T> {

	public String getShortStatus();
	
	public String getTaskName();
	
	public int total();
	
	public int progress();
}
