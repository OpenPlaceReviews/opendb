package org.openplacereviews.opendb.ops.op;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.openplacereviews.opendb.service.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;

@OpenDBOperation(type = OperationsRegistry.OP_TYPE_SYS, name = OpOperation.OP_ID)
public class OpOperation implements OpenDBOperationExec {

	protected static final Log LOGGER = LogFactory.getLog(OpOperation.class);
	
	public static final String OP_ID = "operation";
	public static final String F_NAME = "name";
 	public static final String F_IDENTITY = "identity";
 	public static final String F_TABLE = "table";
 	public static final String F_TABLE_COLUMNS = "table";
	
	private OpDefinitionBean definition;

	@Override
	public String getDescription() {
		return "TODO";
	}

	@Override
	public boolean prepare(OpDefinitionBean definition) {
		this.definition = definition;
		return true;
	}

	@Override
	public boolean execute(JdbcTemplate template) {
		
		return true;
	}

	@Override
	public OpDefinitionBean getDefinition() {
		return definition;
	}

	

}
