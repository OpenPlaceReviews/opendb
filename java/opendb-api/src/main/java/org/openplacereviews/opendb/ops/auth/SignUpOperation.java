package org.openplacereviews.opendb.ops.auth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.IOpenDBOperation;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.ops.OperationsRegistry;
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
		return "This operation signs up new user in DB. Supported fields:"+
		"<br>'auth_method' : authorization method (osm, google, pwd, fb)" +
		"<br>'nickname' : unique nickname" + 
		"<br>list of other fields";
	}

	@Override
	public boolean prepare(OpDefinitionBean definition, StringBuilder errorMessage) {
		this.definition = definition;
		// TODO Auto-generated method stub
		return false;
	}

	// TODO validate 
	@Override
	public boolean execute(JdbcTemplate template, StringBuilder errorMessage) {
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
