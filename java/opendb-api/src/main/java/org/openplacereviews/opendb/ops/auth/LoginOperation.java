package org.openplacereviews.opendb.ops.auth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;

@OpenDBOperation(LoginOperation.OP_ID)
public class LoginOperation implements OpenDBOperationExec {

	protected static final Log LOGGER = LogFactory.getLog(LoginOperation.class);
	
	public static final String OP_ID = "login";
	public static final String F_NAME = "name";
 	public static final String F_KEYGEN_METHOD = "keygen_method";
 	public static final String F_ALGO = "algo";
 	
 	public static final String F_PUBKEY = "pubkey";
 	// not stored in blockchain
 	public static final String F_PRIVATEKEY = "privatekey";
	
	
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
	public boolean prepare(OpDefinitionBean definition) {
		this.definition = definition;
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean execute(JdbcTemplate template) {
		// SecUtils.validateSignature(keyPair, msg, signature)
		// TODO Auto-generated method stub
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
