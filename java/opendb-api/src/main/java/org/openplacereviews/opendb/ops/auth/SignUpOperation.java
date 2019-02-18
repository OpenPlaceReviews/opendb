package org.openplacereviews.opendb.ops.auth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.service.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;

@OpenDBOperation(type=OperationsRegistry.OP_TYPE_AUTH, name=SignUpOperation.OP_ID)
public class SignUpOperation implements OpenDBOperationExec {

	protected static final Log LOGGER = LogFactory.getLog(SignUpOperation.class);
	
	public static final String OP_ID = "signup";
	public static final String F_NAME = "name";
	public static final String F_SALT = "salt";
 	public static final String F_KEYGEN_METHOD = "keygen_method";
 	public static final String F_PUBKEY = "pubkey";
	public static final String F_ALGO = "algo";
	public static final String F_AUTH_METHOD = "auth_method";
	public static final String F_OAUTH_PROVIDER = "oauth_provider";
	public static final String F_OAUTHID_HASH = "oauthid_hash";
	public static final String F_DETAILS = "details";

	public static final String METHOD_OAUTH = "oauth";
	public static final String METHOD_PWD = "pwd";
	public static final String METHOD_PROVIDED = "provided";
	
	private OpDefinitionBean definition;

	@Override
	public String getDescription() {
		return "This operation signs up new user in DB."+
		"<br>This operation must be signed by signup key itself and the login key of the server that can signup users." +
		"<br>Supported fields:"+
		"<br>'name' : unique nickname" +
		"<br>'auth_method' : authorization method (oauth, pwd, provided)" +
		"<br>'pub_key' : public key for assymetric crypthograph" +
		"<br>'algo' : algorithm for assymetric crypthograph" +
		"<br>'keygen_method' : keygen is specified when pwd is used" +
		"<br>'oauthid_hash' : hash for oauth id which is calculated with 'salt'" +
		"<br>'oauth_provider' : oauth provider such as osm, fb, google" +
		"<br>'details' : json with details for spoken languages, avatar, country" +
		"<br>list of other fields";
	}

	@Override
	public boolean prepare(OpDefinitionBean definition) {
		this.definition = definition;
		
		return validateNickname(definition.getName());
	}

	@Override
	public boolean execute(JdbcTemplate template) {
		return true;
	}
	
	private static boolean isAllowedNicknameSymbol(char c) {
		return c == ' ' || c == '$'  || c == '_' ||  
				c == '.' || c == '-' ;
	}
	
	
	public static boolean validateNickname(String name) {
		if(name.trim().length() == 0) {
			return false;
		}
		for(int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);
			if(!Character.isLetter(c) && !Character.isDigit(c) && !isAllowedNicknameSymbol(c)) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public OpDefinitionBean getDefinition() {
		return definition;
	}

	
}
