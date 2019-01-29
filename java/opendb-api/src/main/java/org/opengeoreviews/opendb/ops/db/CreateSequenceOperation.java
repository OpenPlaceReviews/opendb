package org.opengeoreviews.opendb.ops.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opengeoreviews.opendb.Utils;
import org.opengeoreviews.opendb.ops.IOpenDBOperation;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.opengeoreviews.opendb.ops.OpenDBOperation;
import org.opengeoreviews.opendb.ops.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;


@OpenDBOperation(CreateSequenceOperation.OP_ID)
public class CreateSequenceOperation implements IOpenDBOperation {

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
	public boolean prepare(OpDefinitionBean definition, StringBuilder errorMessage) {
		this.definition = definition;
		seqName = definition.getStringValue(FIELD_SEQ_NAME);
		if(!Utils.validateSqlIdentifier(seqName, errorMessage, FIELD_SEQ_NAME, "create sequence")) {
			return false;
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
	public boolean execute(JdbcTemplate template, StringBuilder errorMessage) {
		StringBuilder sql = new StringBuilder("create sequence " + seqName);
		if(minValue != null) {
			sql.append(" MINVALUE ").append(minValue.intValue());
		}
		try {
			LOGGER.info("DDL executed: " + sql);
			template.execute(sql.toString());
		} catch(RuntimeException e) {
			LOGGER.warn("DDL failed: " + e.getMessage(), e);
			errorMessage.append("Failed to execute DDL: " + e.getMessage());
			return false;
		}
		return true;
	}
	
	@Override
	public OpDefinitionBean getDefinition() {
		return definition;
	}
	


}
