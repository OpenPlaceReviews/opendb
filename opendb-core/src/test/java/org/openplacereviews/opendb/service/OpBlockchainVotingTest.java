package org.openplacereviews.opendb.service;

import org.junit.Before;
import org.junit.Test;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import static org.openplacereviews.opendb.ObjectGeneratorTest.generateVotingOperations;
import static org.openplacereviews.opendb.ObjectGeneratorTest.getVotingOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

public class OpBlockchainVotingTest {


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
		for (OpOperation voteOp : getVotingOperations(jsonFormatter, blc)) {
			voteOp.makeImmutable();
			blc.addOperation(voteOp);
		}
		blc.createBlock(serverName, serverKeyPair);
	}

	@Test
	public void testCreatingSysVoteOpWithNotGrantedRoleAdministrator() {
	}

	@Test
	public void testVotingWithNotValidaStateForVote() {

	}

	@Test
	public void testVotingWithNotValidSignedAndVote() {

	}

	@Test
	public void testFinishVoteOpWithWhenSumVotesLess0() {

	}
}
