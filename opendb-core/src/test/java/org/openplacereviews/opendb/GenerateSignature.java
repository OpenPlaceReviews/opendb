package org.openplacereviews.opendb;

import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.JSON_MSG_TYPE;

public class GenerateSignature {
	public static void main(String[] args) throws FailedVerificationException {
		JsonFormatter formatter = new JsonFormatter();
		String msg = "\t{\n" +
				"\t\t\"type\" : \"sys.login\",\n" +
				"\t\t\"signed_by\": \"openplacereviews\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
				"\t\t},\n" +
				"\t\t\"delete\" : [\"f5f8668b05be06d19f91fb80ec0d021c1823058f3e71203a7c79310c0ccff529:0\"],\n" +
				"\t\t\"create\": [{\n" +
				"\t\t\t\"id\": [\"openplacereviews\",\"test_1\"],\n" +
				"\t\t\t\"algo\": \"EC\",\n" +
				"\t\t\t\"auth_method\": \"provided\",\n" +
				"\t\t\t\"pubkey\": \"base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAETxKWXg3jhSxBtYhTxO/zMj6S0jO95ETUehlZ7yR150gFSmxqJcLE4YQDZ6t/Hn13kmaZbhSFMNsAX+kbeUMqbQ==\"\n" +
				"\t\t}],\n" +
				"\t\t\"hash\": \"json:sha256:152176b2efc4c277273884fab432784eb659972e7c56718b82cf557210c65aad\",\n" +
				"\t\t\"signature\": \"ECDSA:base64:MEYCIQDkNeXMJvY7cUPnOhudtsulPBPT6BLRGAz9odiHRgHmaQIhAO3dqFaPbASCsFRWPTZd0XqQocXAf2K8iLn+9EApW0wF\"\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(msg);
		String hash = JSON_MSG_TYPE + ":"
				+ SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, null,
				formatter.opToJsonNoHash(opOperation));

		byte[] hashBytes = SecUtils.getHashBytes(hash);
		String signature = SecUtils.signMessageWithKeyBase64(serverKeyPair, hashBytes, SecUtils.SIG_ALGO_ECDSA, null);
		System.out.println(formatter.opToJsonNoHash(opOperation));
		System.out.println(hash);
		System.out.println(signature);
	}
}
