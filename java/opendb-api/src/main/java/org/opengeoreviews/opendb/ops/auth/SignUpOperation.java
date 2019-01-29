package org.opengeoreviews.opendb.ops.auth;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    public static void main(String[] args) throws Exception {

        KeyPair keyPair = getKeyPair();

        byte[] data = "test".getBytes("UTF8");

        Signature sig = Signature.getInstance("SHA1WithRSA");
        sig.initSign(keyPair.getPrivate());
        sig.update(data);
        byte[] signatureBytes = sig.sign();
        System.out.println("Signature:" + Arrays.toString(signatureBytes));

        
        sig = Signature.getInstance("SHA1WithRSA");
        sig.initVerify(keyPair.getPublic());
        sig.update(data);
        

        System.out.println(sig.verify(signatureBytes));
    }

    private static KeyPair getKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(1024, new SecureRandom("Hello world  + salt".getBytes()));
        System.out.println(Arrays.toString(kpg.genKeyPair().getPublic().getEncoded()));
        System.out.println(Arrays.toString(kpg.genKeyPair().getPrivate().getEncoded()));
//        kpg.initialize(1024);
        return kpg.genKeyPair();
    }
}
