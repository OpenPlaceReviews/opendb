package org.openplacereviews.opendb;

import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.JSON_MSG_TYPE;

public class GenerateSignature {
	public static void main(String[] args) throws FailedVerificationException {
		JsonFormatter formatter = new JsonFormatter();
		String msg = "{\n" +
				"\t\t\"type\" : \"sys.login\",\n" +
				"\t\t\"signed_by\": \"openplacereviews\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
				"\t\t},\n" +
				"\t\t\"delete\" : [[\"openplacereviews\",\"test_1\"]],\n" +
				"\t\t\"create\": [{\n" +
				"\t\t\t\"id\": [\"openplacereviews\",\"test_1\"],\n" +
				"\t\t\t\"algo\": \"EC\",\n" +
				"\t\t\t\"auth_method\": \"provided\",\n" +
				"\t\t\t\"pubkey\": \"base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAETxKWXg3jhSxBtYhTxO/zMj6S0jO95ETUehlZ7yR150gFSmxqJcLE4YQDZ6t/Hn13kmaZbhSFMNsAX+kbeUMqbQ==\"\n" +
				"\t\t}],\n" +
				"\t\t\"hash\": \"json:sha256:5d5d1bb287be77fc38249f95db5b8a4fa10e8ba8df94ac5618bc2d5f3e040ea4\",\n" +
				"\t\t\"signature\": \"ECDSA:base64:MEQCIHn8bCLfzmGpsnBKgbh3oGDrAZnETLR3Qq58m5QssXFbAiA25vZUizf2Dr9GomQ+zuj7JIZc4GW6GoNmP79Bvpdxng==\"\n" +
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
