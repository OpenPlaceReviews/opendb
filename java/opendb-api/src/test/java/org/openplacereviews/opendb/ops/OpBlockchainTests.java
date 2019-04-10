package org.openplacereviews.opendb.ops;

import static org.junit.Assert.assertEquals;

import java.io.InputStreamReader;
import java.security.KeyPair;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.MgmtController;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;

public class OpBlockchainTests {

	public static final String SIMPLE_JSON = "{'a':1, 'b': 'b', 'c' : ['1', '2'], 'e' : {'a': {'a':3}} }";
	String serverName = "openplacereviews:test_1";
	String serverKey = "base64:PKCS#8:MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCAOpUDyGrTPRPDQRCIRXysxC6gCgSTiNQ5nVEjhvsFITA==";
	String serverPublicKey = "base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAETxKWXg3jhSxBtYhTxO/zMj6S0jO95ETUehlZ7yR150gFSmxqJcLE4YQDZ6t/Hn13kmaZbhSFMNsAX+kbeUMqbQ==";
	
	protected String[] BOOTSTRAP_LIST = 
			new String[] {"opr-0-test-user", "std-ops-defintions", "std-roles", "std-validations", "opr-0-test-grant"};
	
	private OpBlockChain blc;
	private JsonFormatter formatter;
	private KeyPair serverKeyPair;
	
	
	@BeforeClass
    public static void beforeAllTestMethods() {
    }
 
    @Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		this.serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverKey, serverPublicKey);
		for (String f : BOOTSTRAP_LIST) {
			OpOperation[] lst = formatter.fromJson(
					new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/" + f + ".json")),
					OpOperation[].class);
			for (OpOperation o : lst) {
				if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
					o.setSignedBy(serverName);
					o = blc.getRules().generateHashAndSign(o, serverKeyPair);
				}
				o.makeImmutable();
				blc.addOperation(o);
			}
		}
	}
 
    
	
	@Test
	public void testSimpleFunctionEval() {
//		assertEquals(3, blc.getObjects(OpBlockchainRules.OP_OPERATION, null));
	}
	
	@Test
	public void testSimpleFunctionEval2() {
		assertEquals("3", "3");
	}
}
