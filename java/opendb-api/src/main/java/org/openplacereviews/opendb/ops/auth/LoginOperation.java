package org.openplacereviews.opendb.ops.auth;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.ECGenParameterSpec;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.crypto.generators.ECKeyPairGenerator;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.generators.SCrypt;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.prng.FixedSecureRandom;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.openplacereviews.opendb.ops.IOpenDBOperation;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;

@OpenDBOperation(LoginOperation.OP_ID)
public class LoginOperation implements IOpenDBOperation {

	protected static final Log LOGGER = LogFactory.getLog(LoginOperation.class);
	
	public static final String OP_ID = "login";
	public static final String F_NAME = "name";
 	public static final String F_KEYGEN_METHOD = "keygen_method";
 	public static final String F_ALGO = "algo";
 	public static final String F_PUBKEY = "pubkey";
 	public static final String F_PUBKEY_FORMAT = "pubkey_format";
 	
 	public static final String F_PRIVATEKEY = "privatekey";
 	public static final String F_PRIVATEKEY_FORMAT = "privatekey_format";
	
	
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
