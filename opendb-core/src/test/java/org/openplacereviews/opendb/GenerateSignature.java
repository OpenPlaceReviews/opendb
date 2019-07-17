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
				"\t\t\"type\" : \"sys.vote\",\n" +
				"\t\t\"signed_by\" : \"openplacereviews:test_1\",\n" +
				"\t\t\"edit\" : [{\n" +
				"\t\t\t\"id\" : [\"vote\", \"osm.place\"],\n" +
				"\t\t\t\"change\" : {\n" +
				"\t\t\t\t\"votes\" : {\n" +
				"\t\t\t\t\t\"append\": {\n" +
				"\t\t\t\t\t\t\"user\" :[\"openplacereviews:test_2\"],\n" +
				"\t\t\t\t\t\t\"vote\" : -1\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t}\n" +
				"\t\t\t},\n" +
				"\t\t\t\"current\" : {}\n" +
				"\t\t}],\n" +
				"\t\t\"hash\": \"json:sha256:8ce4a56dd31ae2a43ac61c33c4ac2211221f0cee04ee0c594b2a06ffac4bc088\",\n" +
				"\t\t\"signature\": \"ECDSA:base64:MEUCIQC3zX/Ug0R8ahxfC4PMrqgam6iVSv3/IlRhqMrtl+0LAwIgLtRRa/6B2Re9ffNpPVauIz736XJg03vggSczqUfqfKg=\"\n" +
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
