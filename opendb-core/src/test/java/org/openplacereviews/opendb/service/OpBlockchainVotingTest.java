package org.openplacereviews.opendb.service;

import org.junit.Before;
import org.junit.Test;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import java.security.KeyPair;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.*;
import static org.openplacereviews.opendb.ops.OpObject.*;
import static org.openplacereviews.opendb.ops.OpOperation.F_EDIT;

public class OpBlockchainVotingTest {

	private static final String OBJ_ID = "123456678";
	private static final String OP_ID = "osm.testplace";
	private static final String VOTE_OBJ_ID = "vote1234567";

	private String username = "openplacereviews1";
	public OpBlockChain blc;

	public JsonFormatter jsonFormatter;

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		jsonFormatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(jsonFormatter, null));
		generateVotingOperations(jsonFormatter, blc);
	}

	@Test
	public void testFullVotingProcess() throws FailedVerificationException {
		int amountOp = 9;
		for (OpOperation voteOp : getVotingOperations(jsonFormatter, blc, amountOp)) {
			voteOp.makeImmutable();
			blc.addOperation(voteOp);
		}
		blc.createBlock(serverName, serverKeyPair);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingSysVoteOpWithNotGrantedRoleAdministrator() throws FailedVerificationException {
		int amountOp = 2;
		for (OpOperation voteOp : getVotingOperations(jsonFormatter, blc, amountOp)) {
			voteOp.makeImmutable();
			blc.addOperation(voteOp);
		}
		blc.createBlock(serverName, serverKeyPair);

		blc.addOperation(generateUserWithoutRoles(username));
		blc.addOperation(generateCreateVoteOpWithUser(username));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testVotingWithNotValidStateForVote() throws FailedVerificationException {
		int amountOp = 3;
		for (OpOperation voteOp : getVotingOperations(jsonFormatter, blc, amountOp)) {
			voteOp.makeImmutable();
			blc.addOperation(voteOp);
		}
		blc.createBlock(serverName, serverKeyPair);

		blc.addOperation(generateUserWithoutRoles(username));
		blc.addOperation(generateVoteOpWithVoteState(username, username, 2));
	}

	@Test
	public void testVotingWithValidStateForVote() throws FailedVerificationException {
		int amountOp = 3;
		for (OpOperation voteOp : getVotingOperations(jsonFormatter, blc, amountOp)) {
			voteOp.makeImmutable();
			blc.addOperation(voteOp);
		}
		blc.createBlock(serverName, serverKeyPair);

		blc.addOperation(generateUserWithoutRoles(username));
		blc.addOperation(generateVoteOpWithVoteState(username, username, 1));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testVotingWithNotValidSignedAndVote() throws FailedVerificationException {
		int amountOp = 3;
		for (OpOperation voteOp : getVotingOperations(jsonFormatter, blc, amountOp)) {
			voteOp.makeImmutable();
			blc.addOperation(voteOp);
		}
		blc.createBlock(serverName, serverKeyPair);

		blc.addOperation(generateUserWithoutRoles(username));
		blc.addOperation(generateVoteOpWithVoteState(username, username + 1, 1));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFinishVoteOpWithWhenSumVotesLess0() throws FailedVerificationException {
		int amountOp = 3;
		for (OpOperation voteOp : getVotingOperations(jsonFormatter, blc, amountOp)) {
			voteOp.makeImmutable();
			blc.addOperation(voteOp);
		}
		blc.createBlock(serverName, serverKeyPair);

		OpOperation finishVoteOp = getVotingOperations(jsonFormatter,blc, 6).get(5);
		finishVoteOp.makeImmutable();
		blc.addOperation(finishVoteOp);
	}

	private OpOperation generateVoteOpWithVoteState(String signed, String vote, Integer state) throws FailedVerificationException {
		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
		String password = "149814981498a";
		KeyPair keyPair = SecUtils.generateKeyPairFromPassword(SecUtils.ALGO_EC, keyGen, username, password);

		OpOperation voteOp = new OpOperation();
		voteOp.setType(OP_VOTE);
		voteOp.setSignedBy(signed);

		OpObject edit = new OpObject();
		edit.setId("vote", "osm.place");
		Map<String, Object> map = new TreeMap<>();
		Map<String, Object> mapObject = new TreeMap<>();
		mapObject.put("set", state);
		map.put("votes." + vote, mapObject);
		edit.putObjectValue("change", map);
		edit.putObjectValue("current", Collections.emptyMap());

		voteOp.addEdited(edit);
		blc.getRules().generateHashAndSign(voteOp, keyPair);
		voteOp.makeImmutable();

		return voteOp;
	}

	private OpOperation generateCreateVoteOpWithUser(String username) throws FailedVerificationException {
		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
		String password = "149814981498a";
		KeyPair keyPair = SecUtils.generateKeyPairFromPassword(SecUtils.ALGO_EC, keyGen, username, password);

		OpOperation voteOp = new OpOperation();
		voteOp.setType(OP_VOTE);
		voteOp.setSignedBy(username);

		OpObject createObj = new OpObject();
		createObj.setId(VOTE_OBJ_ID);
		createObj.putObjectValue(F_STATE, F_OPEN);
		createObj.putObjectValue(F_VOTES, Collections.EMPTY_MAP);

		TreeMap<String, Object> editOpObj = new TreeMap<>();
		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> change = new TreeMap<>();
		change.put("lon", "increment");
		change.put("lat", "delete");
		TreeMap<String, Object> appendObj = new TreeMap<>();
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", 2222222);
		tagsObject.put("k", 333333333);
		appendObj.put("append", tagsObject);
		change.put("tags", appendObj);
		TreeMap<String, Object> current = new TreeMap<>();
		current.put("lon", 12345);
		current.put("lat", "222EC");
		editObj.putObjectValue(F_CHANGE, change);
		editObj.putObjectValue(F_CURRENT, current);

		editOpObj.put(F_TYPE, OP_ID);
		editOpObj.put(F_EDIT, Arrays.asList(editObj));
		createObj.putObjectValue(F_OP, editOpObj);

		voteOp.addCreated(createObj);

		blc.getRules().generateHashAndSign(voteOp, keyPair);
		voteOp.makeImmutable();

		return voteOp;
	}

	private OpOperation generateUserWithoutRoles(String username) throws FailedVerificationException {
		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
		String password = "149814981498a";

		KeyPair keyPair = SecUtils.generateKeyPairFromPassword(SecUtils.ALGO_EC, keyGen, username, password);

		OpObject opObject = new OpObject();
		opObject.setId(username);
		opObject.putStringValue(F_TYPE, OP_SIGNUP);
		opObject.putStringValue(OpObject.F_NAME, username);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		OpOperation opOperation = new OpOperation();
		opOperation.setSignedBy(username);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		return opOperation;
	}
}
