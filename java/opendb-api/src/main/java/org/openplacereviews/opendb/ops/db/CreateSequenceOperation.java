package org.openplacereviews.opendb.ops.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.Utils;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;


@OpenDBOperation(type=OperationsRegistry.OP_TYPE_DDL, name=CreateSequenceOperation.OP_ID)
public class CreateSequenceOperation implements OpenDBOperationExec {

	protected static final Log LOGGER = LogFactory.getLog(CreateSequenceOperation.class);
	public static final String OP_ID = "create_sequence";
	
	private static final String FIELD_SEQ_NAME = "name";
	private static final String FIELD_SEQ_MINVALUE = "minvalue";
	private OpDefinitionBean definition;
	private String seqName;
	private Number minValue;
	
	
	@Override
	public String getName() {
		return OP_ID;
	}
	
	@Override
	public String getType() {
		return OperationsRegistry.OP_TYPE_DDL;
	}
	
	
	@Override
	public boolean prepare(OpDefinitionBean definition) {
		this.definition = definition;
		seqName = definition.getStringValue(FIELD_SEQ_NAME);
		StringBuilder errorMessage = new StringBuilder();
		if(!Utils.validateSqlIdentifier(seqName, errorMessage, FIELD_SEQ_NAME, "create sequence")) {
			throw new IllegalArgumentException(errorMessage.toString());
		}
		minValue = definition.getNumberValue(FIELD_SEQ_MINVALUE);
		return true;
	}
	
	

	@Override
	public String getDescription() {
		return "This operation creates sequence in DB. Supported fields:"+
				"<br>'comment' : comment for op " +
				"<br>'name' : sequence name" + 
				"<br>'minvalue' : minimum value";
	}

	@Override
	public boolean execute(JdbcTemplate template) {
		StringBuilder sql = new StringBuilder("create sequence " + seqName);
		if(minValue != null) {
			sql.append(" MINVALUE ").append(minValue.intValue());
		}
		try {
			LOGGER.info("DDL executed: " + sql);
			template.execute(sql.toString());
		} catch (RuntimeException e) {
			LOGGER.warn("DDL failed: " + e.getMessage(), e);
			throw e;
		}
		return true;
	}
	
	@Override
	public OpDefinitionBean getDefinition() {
		return definition;
	}
	


}
