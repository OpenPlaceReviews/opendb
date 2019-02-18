package org.openplacereviews.opendb.ops;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.UsersAndRolesRegistry;

public class LoginOperation  {

	protected static final Log LOGGER = LogFactory.getLog(LoginOperation.class);
	
	public static final String OP_ID = "login";
	public static final String F_NAME = "name";
 	public static final String F_ALGO = "algo";
 	
 	public static final String F_PUBKEY = "pubkey";
 	// not stored in blockchain
 	public static final String F_PRIVATEKEY = "privatekey";
	
	

	public String getDescription() {
		return "This operation logins an existing user to a specific 'site' (named login). " + 
	    "In case user was logged in under such name the previous login key pair will become invalid." +
		"<br>This operation must be signed by signup key of the user." +
	    "<br>Supported fields:"+
	    "<br>'name' : unique name for a user of the site or purpose of login" +
		"<br>'pub_key' : public key for assymetric crypthograph" +
		"<br>'algo' : algorithm for assymetric crypthograph" +
		"<br>'keygen_method' : later could be used to explain how the key was calculated" +
		"<br>'details' : json with details" +
		"<br>list of other fields";
	}

	public boolean prepare(OpDefinitionBean definition) {
		return SignUpOperation.validateNickname(UsersAndRolesRegistry.getSiteFromUser(definition.getName())) &&
				SignUpOperation.validateNickname(UsersAndRolesRegistry.getNicknameFromUser(definition.getName()));
	}



}
