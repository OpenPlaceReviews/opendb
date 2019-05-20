package org.openplacereviews.opendb;

import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Arrays;

public class VariableHelperTest {

	public static String serverName = "openplacereviews:test_1";
	public static String serverKey = "base64:PKCS#8:MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCAOpUDyGrTPRPDQRCIRXysxC6gCgSTiNQ5nVEjhvsFITA==";
	public static String serverPublicKey = "base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAETxKWXg3jhSxBtYhTxO/zMj6S0jO95ETUehlZ7yR150gFSmxqJcLE4YQDZ6t/Hn13kmaZbhSFMNsAX+kbeUMqbQ==";
	public static KeyPair serverKeyPair;
	public static ArrayList<String> tables = new ArrayList<>(Arrays.asList("opendb_settings", "blocks", "operations", "op_deleted", "objs","operations_trash", "blocks_trash"));

	static {
		try {
			serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverKey, serverPublicKey);
		} catch (FailedVerificationException e) {
			e.printStackTrace();
		}
	}

}
