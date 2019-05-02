package org.openplacereviews.opendb;

import org.openplacereviews.opendb.api.MgmtController;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.io.InputStreamReader;
import java.security.KeyPair;

import static org.openplacereviews.opendb.VariableHelperTest.serverName;

public class ObjectGeneratorTest {

	private static String[] BOOTSTRAP_LIST =
			new String[]{"opr-0-test-user", "std-ops-defintions", "std-roles", "std-validations", "opr-0-test-grant"};

	public static void generateOperations(JsonFormatter formatter, OpBlockChain blc, KeyPair serverKeyPair) throws FailedVerificationException {
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

	public static void generateHashAndSignForOperation(OpOperation opOperation, KeyPair keyPair, OpBlockChain blc, boolean signedBy) throws FailedVerificationException {
		if (signedBy) {
			opOperation.setSignedBy(serverName);
		}

		blc.getRules().generateHashAndSign(opOperation, keyPair);
	}

	/**
	 * Allows to generate JSON with big size for creating Error Type.OP_SIZE_IS_EXCEEDED
	 * @return - String Json
	 */
	public static String generateBigJSON() {
		StringBuilder startOperation =
				new StringBuilder("{\n" +
						"\t\t\"type\" : \"sys.grant\",\n" +
						"\t\t\"ref\" : {\n" +
						"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
						"\t\t},\n" +
						"\t\t\"new\" : [");

		while (startOperation.length() <= OpBlockchainRules.MAX_OP_SIZE_MB) {
			for(int i = 0; i < 100; i++) {
				startOperation.append("\t\t{ \n" + "\t\t\t\"id\" : [\"openplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviews").append(startOperation.length()).append(i).append("\"],\n").append("\t\t\t\"roles\" : [\"owner\"]\n").append("\t\t},");
			}
		}

		startOperation.append("\t\t{ \n" + "\t\t\t\"id\" : [\"openplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviews\"],\n" + "\t\t\t\"roles\" : [\"owner\"]\n" + "\t\t}]\n" + "\t}");

		return startOperation.toString();
	}
}
