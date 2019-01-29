package org.opengeoreviews.opendb.ops.auth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opengeoreviews.opendb.SecUtils;
import org.opengeoreviews.opendb.ops.IOpenDBOperation;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.opengeoreviews.opendb.ops.OpenDBOperation;
import org.opengeoreviews.opendb.ops.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;

@OpenDBOperation(SignUpOperation.OP_ID)
public class SignUpOperation implements IOpenDBOperation {

	protected static final Log LOGGER = LogFactory.getLog(SignUpOperation.class);
	
	public static final String OP_ID = "signup";
	private OpDefinitionBean definition;

	@Override
	public String getName() {
		return OP_ID;
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean prepare(OpDefinitionBean definition, StringBuilder errorMessage) {
		this.definition = definition;
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(JdbcTemplate template, StringBuilder errorMessage) {
		// TODO Auto-generated method stub
		// TODO make separate api to create keys
		// SecUtils.validateSignature(keyPair, msg, signature)
		return false;
	}
	
	
	
	

	@Override
	public OpDefinitionBean getDefinition() {
		return definition;
	}

	
	@Override
	public String getType() {
		return OperationsRegistry.OP_TYPE_AUTH;
	}

}
