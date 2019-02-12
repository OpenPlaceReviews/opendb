package org.openplacereviews.opendb.ops;

import org.springframework.jdbc.core.JdbcTemplate;

public interface OpenDBOperationExec {

	/** 
	 * Operation name / id
	 */
	public String getName();
	
	/**
	 * Operation type 
	 */
	public String getType();
	
	/**
	 *  Will be used for documentation
	 */
	public String getDescription();
	
	/**
	 * Prepare operation will be always called before execution, 
	 * so some initial preparation could be done in that method 
	 * @return whether operation is valid for execution or not
	 * error message describes if there is an error 
	 */
	public boolean prepare(OpDefinitionBean definition);
	
	
	/**
	 * Executes operation and returns whether it was successful 
	 * and should be included in the block or not.
	 * @return true if everything was successful
	 */
	public boolean execute(JdbcTemplate template);

	
	/**
	 * @return definition of the operation
	 */
	public OpDefinitionBean getDefinition();

	
	
	
	
}
