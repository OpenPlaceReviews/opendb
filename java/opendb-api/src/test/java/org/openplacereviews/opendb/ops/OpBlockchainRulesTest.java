package org.openplacereviews.opendb.ops;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.security.KeyPair;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateBigJSON;
import static org.openplacereviews.opendb.VariableHelperTest.*;

public class OpBlockchainRulesTest {

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	private OpBlockChain blc;
	private JsonFormatter formatter;
	private KeyPair serverKeyPair;

	@Before
	public void beforeEachTestMethod() throws Exception {
		formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		this.serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverKey, serverPublicKey);
		generateOperations(formatter, blc, serverKeyPair);
	}

	/**
	 * Success adding new operation
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperation() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.grant\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
				"\t\t},\n" +
				"\t\t\"new\" : [{ \n" +
				"\t\t\t\"id\" : [\"openplacereviews1\"],\n" +
				"\t\t\t\"roles\" : [\"owner\"]\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * Expect ErrorType.OP_HASH_IS_NOT_CORRECT
	 */
	@Test
	public void testAddOperationExpectError_OpHashIsNotCorrect() {
		OpOperation opOperation = new OpOperation();
		opOperation.makeImmutable();

		blc.removeAllQueueOperations();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expect ErrorType.OP_HASH_IS_DUPLICATED
	 */
	@Test
	public void testAddOperationExpectError_OpHashIsDuplicated() {
		OpOperation opOperation = blc.getQueueOperations().getFirst();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expect ErrorType.MGMT_CANT_DELETE_NON_LAST_OPERATIONS
	 */
	@Test
	public void testAddOperationExpectError_MgmtCantDeleteNonLastOperations() {
		OpOperation opOperation = blc.getQueueOperations().getFirst();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.removeQueueOperations(new HashSet<>(Collections.singletonList(opOperation.getRawHash())));
	}

	/**
	 * Expect ErrorType.REF_OBJ_NOT_FOUND
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareReferencedObjectsExpectError_RefObjNotFound() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.grant\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.login1\",\"openplacereviews\",\"test_1\"]\n" +
				"\t\t},\n" +
				"\t\t\"new\" : [{ \n" +
				"\t\t\t\"id\" : [\"openplacereviews:test_2\"],\n" +
				"\t\t\t\"roles\" : [\"master\", \"administrator\"]\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		opOperation.setSignedBy(serverName);
		opOperation = blc.getRules().generateHashAndSign(opOperation, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expect ErrorType.DEL_OBJ_NOT_FOUND
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareDeletedObjectsExpectError_DelObjNotFound() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.login\",\n" +
				"\t\t\"signed_by\": \"openplacereviews\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
				"\t\t},\n" +
				"\t\t\"old\" : [\"fefffd95ccaa8b2545f2c5b8e1e7ae8c7d8f530b8d61be60df2345d74102c801:0\"],\n" +
				"\t\t\"signature\": \"ECDSA:base64:MEYCIQDCuwakI7jd0bExEDnnKc4X41oS2hbj0XwRfuSgXqu6/gIhAKQSlPt9amGgHz20yiES87vOt4i3/BFDu3IrGgIlz8AM\"\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		opOperation.setSignedBy(serverName);
		opOperation = blc.getRules().generateHashAndSign(opOperation, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expect ErrorType.DEL_OBJ_DOUBLE_DELETED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareDeletedObjectsExpectError_DelObjDoubleDeleted() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		String operation = "" +
				"{\n" +
				"\t\t\"type\" : \"sys.login\",\n" +
				"\t\t\"signed_by\": \"openplacereviews\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
				"\t\t},\n" +
				"\t\t\"old\" : [\"fefffd95ccaa8b2545f2c5b8e1e7ae8c7d8f530b8d61be60df2345d74102c802:0\"],\n" +
				"\t\t\"signature\": \"ECDSA:base64:MEYCIQDCuwakI7jd0bExEDnnKc4X41oS2hbj0XwRfuSgXqu6/gIhAKQSlPt9amGgHz20yiES87vOt4i3/BFDu3IrGgIlz8AM\"\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		opOperation.setSignedBy(serverName);
		opOperation = blc.getRules().generateHashAndSign(opOperation, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expect ErrorType.NEW_OBJ_DOUBLE_CREATED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareNoNewDuplicatedObjectsExpectError_NewObjDoubleCreated() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.grant\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.login1\",\"openplacereviews\",\"test_1\"]\n" +
				"\t\t},\n" +
				"\t\t\"new\" : [{ \n" +
				"\t\t\t\"id\" : [\"openplacereviews:test_1\"],\n" +
				"\t\t\t\"roles\" : [\"master\", \"administrator\"]\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		opOperation.setSignedBy(serverName);
		opOperation = blc.getRules().generateHashAndSign(opOperation, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Success validation block
	 */
	@Test
	public void testValidateBlock() throws FailedVerificationException {
		assertNotNull(blc.createBlock(serverName, serverKeyPair));
	}

	/**
	 * Expect ErrorType.BLOCK_PREV_HASH
	 */
	@Ignore
	@Test
	public void testValidateBlockExpectError_BlockPrevHash() {
	}

	/**
	 * Expect ErrorType.BLOCK_PREV_ID
	 */
	@Ignore
	@Test
	public void testValidateBlockExpectError_BlockPrevId() {

	}

	/**
	 * Expect ErrorType.BLOCK_HASH_IS_DUPLICATED
	 */
	@Ignore
	@Test
	public void testValidateBlockExpectError_BlockHashIsDuplicated() {

	}

	/**
	 * Expect ErrorType.BLOCK_EMPTY
	 */
	@Test
	public void testValidateBlockExpectError_BlockEmpty() throws FailedVerificationException {
		assertNotNull(blc.createBlock(serverName, serverKeyPair));

		exceptionRule.expect(IllegalArgumentException.class);
		blc.createBlock(serverName, serverKeyPair);
	}

	/**
	 * Expect ErrorType.BLOCK_MERKLE_TREE_FAILED
	 */
	@Ignore
	@Test
	public void testValidateBlockExpectError_BlockMerkleTreeFailed() {

	}

	/**
	 * Expect ErrorType.BLOCK_SIG_MERKLE_TREE_FAILED
	 */
	@Ignore
	@Test
	public void testValidateBlockExpectError_BlockSigMerkleTreeFailed() {

	}

	/**
	 * Expect ErrorType.BLOCK_SIGNATURE_FAILED
	 */
	@Test
	public void testValidateBlockExpectError_BlockSignatureFailed() throws FailedVerificationException {
		exceptionRule.expect(IllegalArgumentException.class);
		blc.createBlock(serverName + 1, serverKeyPair);
	}

	/**
	 * Expect ErrorType.BLOCK_HASH_FAILED
	 */
	@Ignore
	@Test
	public void testValidateBlockExpectError_BlockHashFailed() {
	}

	/**
	 * Expect ErrorType.OP_SIGNATURE_FAILED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateSignaturesExpectError_OpSignatureFailed() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.signup\",\n" +
				"\t\t\"new\": [{\t\n" +
				"\t\t\t\"id\": [\"openplacereviews1\"],\n" +
				"\t\t\t\"name\" : \"openplacereviews1\",\n" +
				"\t\t\t\"algo\": \"EC\",\n" +
				"\t\t\t\"auth_method\": \"oauth\",\n" +
				"\t\t\t\"pwd\": \"149814981498a\"\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.remove(OpOperation.F_SIGNATURE);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.validateOperation(opOperation);
	}

	/**
	 * Expect ErrorType.OP_SIGNATURE_FAILED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateSignaturesFirstSignupCouldBeSignedByItselfExpectError_OpSignatureFailed() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.signup\",\n" +
				"\t\t\"signed_by\": \"openplacereviews:test_1\",\n" +
				"\t\t\"new\": [{\t\n" +
				"\t\t\t\"id\": [\"openplacereviews1\"],\n" +
				"\t\t\t\"name\" : \"openplacereviews1\",\n" +
				"\t\t\t\"algo\": \"EC\",\n" +
				"\t\t\t\"auth_method\": \"oauth\",\n" +
				"\t\t\t\"pwd\": \"149814981498a\"\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		opOperation.setSignedBy(serverName + 1);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, false);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.validateOperation(opOperation);
	}

	/**
	 * Success validateOperations
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateOp() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.signup\",\n" +
				"\t\t\"new\": [{\t\n" +
				"\t\t\t\"id\": [\"openplacereviews1\"],\n" +
				"\t\t\t\"name\" : \"openplacereviews1\",\n" +
				"\t\t\t\"algo\": \"EC\",\n" +
				"\t\t\t\"auth_method\": \"oauth\",\n" +
				"\t\t\t\"pwd\": \"149814981498a\"\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		assertTrue(blc.validateOperation(opOperation));
	}

	/**
	 *  Expect ErrorType.OP_HASH_IS_NOT_CORRECT
	 */
	@Test
	public void testValidateOpExpectError_OpHashIsNotCorrect() {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.role\",\n" +
				"\t\t\"hash\" : \"json:sha256:e55df720278460a277425e6331f08c436160a64db639051a82d79015e10dff03\",\n" +
				"\t\t\"new\" : [{ \n" +
				"\t\t\t\"id\"\t: [\"subadmin\"],\n" +
				"\t\t\t\"comment\"   : \"Role master and only owner could change it\",\n" +
				"\t\t\t\"owner_role\" : \"administrator\",\n" +
				"\t\t\t\"super_roles\" : [\"owner\"]\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.validateOperation(opOperation);
	}

	/**
	 * Expect ErrorType.OP_SIZE_IS_EXCEEDED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateOpExpectError_OpSizeIsExceeded() throws FailedVerificationException {
		OpOperation opOperation = formatter.parseOperation(generateBigJSON());
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * 	Expect ErrorType.OP_INVALID_VALIDATE_EXPRESSION
	 */
	@Ignore
	@Test
	public void testValidateRulesExpectError_OpInvalidValidateExpression() {

	}

	/**
	 *  Expect ErrorType.OP_ROLE_SUPER_ROLE_CIRCULAR_REF
	 */
	@Ignore
	@Test
	public void testValidateRulesExpectError_OpRoleSuperRoleCircularRef() throws FailedVerificationException {
		String operation = "";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 *  Expect ErrorType.OP_ROLE_SUPER_ROLE_DOESNT_EXIST
	 */
	@Test
	public void testValidateRulesExpectError_OpRoleSuperRoleDoesntExist() throws FailedVerificationException {
		String operation =
				"{\n" +
						"\t\t\"type\"  : \"sys.role\",\n" +
						"\t\t\"new\" : [{\n" +
						"\t\t\t\"id\" : [\"owner1\"],\n" +
						"\t\t\t\"comment\" : \"Owner role is a super role that nobody could change it\",\n" +
						"\t\t\t\"owner_role\" : \"owner\",\n" +
						"\t\t\t\"super_roles\": [\"owner1\"]\n" +
						"\t\t}]\n" +
						"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 *  Expect ErrorType.OP_GRANT_ROLE_DOESNT_EXIST
	 */
	@Test
	public void testValidateRulesExpectError_OpGrantRoleDoesntExist() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.grant\",\n" +
				"\t\t\"ref\" : {\n" +
				"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
				"\t\t},\n" +
				"\t\t\"new\" : [{ \n" +
				"\t\t\t\"id\" : [\"openplacereviews_test\"],\n" +
				"\t\t\t\"roles\" : [\"owner1\"]\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}
}