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
				"\t\t\"type\"  : \"sys.signup1\",\n" +
				"\t\t\"signed_by\": \"openplacereviews1\",\n" +
				"\t\t\"new\": [{\n" +
				"\t\t\t\"id\": [\"openplacereviews11\"],\n" +
				"\t\t\t\"name\" : \"openplacereviews12\",\n" +
				"\t\t\t\"osm_id\":\"123234\",\n" +
				"\t\t\t\"algo\": \"EC\",\n" +
				"\t\t\t\"auth_method\": \"pwd\",\n" +
				"\t\t\t\"pubkey\": \"base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAETxKWXg3jhSxBtYhTxO/zMj6S0jO95ETUehlZ7yR150gFSmxqJcLE4YQDZ6t/Hn13kmaZbhSFMNsAX+kbeUMqbQ==\"\n" +
				"\t\t}]\n" +
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
