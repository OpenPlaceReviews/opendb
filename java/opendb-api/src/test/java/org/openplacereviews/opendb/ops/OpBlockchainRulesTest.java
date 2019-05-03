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
import java.util.*;

import static org.junit.Assert.*;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.*;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.BLOCK_VERSION;

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
	 * Expected ErrorType.OP_HASH_IS_NOT_CORRECT
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
	 * Expected ErrorType.OP_HASH_IS_DUPLICATED
	 */
	@Test
	public void testAddOperationExpectError_OpHashIsDuplicated() {
		OpOperation opOperation = blc.getQueueOperations().getFirst();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expected ErrorType.MGMT_CANT_DELETE_NON_LAST_OPERATIONS
	 */
	@Test
	public void testAddOperationExpectError_MgmtCantDeleteNonLastOperations() {
		OpOperation opOperation = blc.getQueueOperations().getFirst();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.removeQueueOperations(new HashSet<>(Collections.singletonList(opOperation.getRawHash())));
	}

	/**
	 * Expected ErrorType.REF_OBJ_NOT_FOUND
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
	 * Expected ErrorType.DEL_OBJ_NOT_FOUND
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
	 * Expected ErrorType.DEL_OBJ_DOUBLE_DELETED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareDeletedObjectsExpectError_DelObjDoubleDeleted() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		String operation =
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
	 * Expected ErrorType.NEW_OBJ_DOUBLE_CREATED
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
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlock() throws FailedVerificationException {
		assertNotNull(blc.createBlock(serverName, serverKeyPair));
	}

	/**
	 * Expected ErrorType.BLOCK_PREV_HASH
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockPrevHash() throws FailedVerificationException {
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		exceptionRule.expect(IllegalArgumentException.class);
		blc.replicateBlock(opBlock);
	}

	/**
	 * Expected ErrorType.BLOCK_PREV_ID
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockPrevId() throws FailedVerificationException {
		Deque<OpOperation> dequeOperations = blc.getQueueOperations();
		assertFalse(dequeOperations.isEmpty());

		List<OpOperation> opOperations = new ArrayList<>();
		Set<String> operationsToDelete = new HashSet<>();

		Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
		selectOperations(opOperations, operationsToDelete, iterator);

		Set<String> removedOperations = blc.removeQueueOperations(operationsToDelete);
		assertEquals(5, removedOperations.size());

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlock block = new OpBlock();
		block.operations.addAll(opOperations);
		block.setDate(OpBlock.F_DATE, System.currentTimeMillis());
		block.putObjectValue(OpBlock.F_BLOCKID, opBlock.getBlockId());
		block.putStringValue(OpBlock.F_PREV_BLOCK_HASH, opBlock.getFullHash());
		block.putStringValue(OpBlock.F_MERKLE_TREE_HASH, blc.getRules().calculateMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH, blc.getRules().calculateSigMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIGNED_BY, serverName);
		block.putObjectValue(OpBlock.F_VERSION, BLOCK_VERSION);
		block.putStringValue(OpBlock.F_DETAILS, "");
		block.putStringValue(OpBlock.F_HASH, blc.getRules().calculateHash(block));
		if (serverKeyPair != null) {
			byte[] hashBytes = SecUtils.getHashBytes(block.getFullHash());
			block.putStringValue(OpBlock.F_SIGNATURE,
					SecUtils.signMessageWithKeyBase64(serverKeyPair, hashBytes, SecUtils.SIG_ALGO_ECDSA, null));
		}
		block.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.replicateBlock(block);
	}

	/**
	 * Expected ErrorType.BLOCK_HASH_IS_DUPLICATED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockHashIsDuplicated() throws FailedVerificationException {
		Deque<OpOperation> dequeOperations = blc.getQueueOperations();
		assertFalse(dequeOperations.isEmpty());

		List<OpOperation> opOperations = new ArrayList<>();
		Set<String> operationsToDelete = new HashSet<>();

		Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
		selectOperations(opOperations, operationsToDelete, iterator);

		Set<String> removedOperations = blc.removeQueueOperations(operationsToDelete);
		assertEquals(5, removedOperations.size());

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlock block = new OpBlock();
		block.operations.addAll(opOperations);
		block.setDate(OpBlock.F_DATE, System.currentTimeMillis());
		block.putObjectValue(OpBlock.F_BLOCKID, opBlock.getBlockId() + 1);
		block.putStringValue(OpBlock.F_PREV_BLOCK_HASH, opBlock.getFullHash());
		block.putStringValue(OpBlock.F_MERKLE_TREE_HASH, blc.getRules().calculateMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH, blc.getRules().calculateSigMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIGNED_BY, serverName);
		block.putObjectValue(OpBlock.F_VERSION, BLOCK_VERSION);
		block.putStringValue(OpBlock.F_DETAILS, "");
		block.putStringValue(OpBlock.F_HASH, opBlock.getRawHash());
		if (serverKeyPair != null) {
			byte[] hashBytes = SecUtils.getHashBytes(block.getFullHash());
			block.putStringValue(OpBlock.F_SIGNATURE,
					SecUtils.signMessageWithKeyBase64(serverKeyPair, hashBytes, SecUtils.SIG_ALGO_ECDSA, null));
		}
		block.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.replicateBlock(block);

	}

	/**
	 * Expected ErrorType.BLOCK_EMPTY
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockEmpty() throws FailedVerificationException {
		assertNotNull(blc.createBlock(serverName, serverKeyPair));

		exceptionRule.expect(IllegalArgumentException.class);
		blc.createBlock(serverName, serverKeyPair);
	}

	/**
	 * Expected ErrorType.BLOCK_MERKLE_TREE_FAILED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockMerkleTreeFailed() throws FailedVerificationException {
		Deque<OpOperation> dequeOperations = blc.getQueueOperations();
		assertFalse(dequeOperations.isEmpty());

		List<OpOperation> opOperations = new ArrayList<>();
		Set<String> operationsToDelete = new HashSet<>();

		Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
		selectOperations(opOperations, operationsToDelete, iterator);

		Set<String> removedOperations = blc.removeQueueOperations(operationsToDelete);
		assertEquals(5, removedOperations.size());

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlock block = new OpBlock();
		block.operations.addAll(opOperations);
		block.setDate(OpBlock.F_DATE, System.currentTimeMillis());
		block.putObjectValue(OpBlock.F_BLOCKID, opBlock.getBlockId() + 1);
		block.putStringValue(OpBlock.F_PREV_BLOCK_HASH, opBlock.getFullHash());
		block.putStringValue(OpBlock.F_MERKLE_TREE_HASH, blc.getRules().calculateSigMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH, blc.getRules().calculateSigMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIGNED_BY, serverName);
		block.putObjectValue(OpBlock.F_VERSION, BLOCK_VERSION);
		block.putStringValue(OpBlock.F_DETAILS, "");
		block.putStringValue(OpBlock.F_HASH, blc.getRules().calculateHash(block));
		if (serverKeyPair != null) {
			byte[] hashBytes = SecUtils.getHashBytes(block.getFullHash());
			block.putStringValue(OpBlock.F_SIGNATURE,
					SecUtils.signMessageWithKeyBase64(serverKeyPair, hashBytes, SecUtils.SIG_ALGO_ECDSA, null));
		}
		block.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.replicateBlock(block);
	}

	/**
	 * Expected ErrorType.BLOCK_SIG_MERKLE_TREE_FAILED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockSigMerkleTreeFailed() throws FailedVerificationException {
		Deque<OpOperation> dequeOperations = blc.getQueueOperations();
		assertFalse(dequeOperations.isEmpty());

		List<OpOperation> opOperations = new ArrayList<>();
		Set<String> operationsToDelete = new HashSet<>();

		Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
		selectOperations(opOperations, operationsToDelete, iterator);

		Set<String> removedOperations = blc.removeQueueOperations(operationsToDelete);
		assertEquals(5, removedOperations.size());

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlock block = new OpBlock();
		block.operations.addAll(opOperations);
		block.setDate(OpBlock.F_DATE, System.currentTimeMillis());
		block.putObjectValue(OpBlock.F_BLOCKID, opBlock.getBlockId() + 1);
		block.putStringValue(OpBlock.F_PREV_BLOCK_HASH, opBlock.getFullHash());
		block.putStringValue(OpBlock.F_MERKLE_TREE_HASH, blc.getRules().calculateMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH, blc.getRules().calculateMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIGNED_BY, serverName);
		block.putObjectValue(OpBlock.F_VERSION, BLOCK_VERSION);
		block.putStringValue(OpBlock.F_DETAILS, "");
		block.putStringValue(OpBlock.F_HASH, blc.getRules().calculateHash(block));
		if (serverKeyPair != null) {
			byte[] hashBytes = SecUtils.getHashBytes(block.getFullHash());
			block.putStringValue(OpBlock.F_SIGNATURE,
					SecUtils.signMessageWithKeyBase64(serverKeyPair, hashBytes, SecUtils.SIG_ALGO_ECDSA, null));
		}
		block.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.replicateBlock(block);
	}

	/**
	 * Expected ErrorType.BLOCK_SIGNATURE_FAILED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockSignatureFailed() throws FailedVerificationException {
		exceptionRule.expect(IllegalArgumentException.class);
		blc.createBlock(serverName + 1, serverKeyPair);
	}

	/**
	 * Expected ErrorType.BLOCK_HASH_FAILED
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockHashFailed() throws FailedVerificationException {
		Deque<OpOperation> dequeOperations = blc.getQueueOperations();
		assertFalse(dequeOperations.isEmpty());

		List<OpOperation> opOperations = new ArrayList<>();
		Set<String> operationsToDelete = new HashSet<>();

		Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
		selectOperations(opOperations, operationsToDelete, iterator);

		Set<String> removedOperations = blc.removeQueueOperations(operationsToDelete);
		assertEquals(5, removedOperations.size());

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlock block = new OpBlock();
		block.operations.addAll(opOperations);
		block.setDate(OpBlock.F_DATE, System.currentTimeMillis());
		block.putObjectValue(OpBlock.F_BLOCKID, opBlock.getBlockId() + 1);
		block.putStringValue(OpBlock.F_PREV_BLOCK_HASH, opBlock.getFullHash());
		block.putStringValue(OpBlock.F_MERKLE_TREE_HASH, blc.getRules().calculateMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH, blc.getRules().calculateSigMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIGNED_BY, serverName);
		block.putObjectValue(OpBlock.F_VERSION, BLOCK_VERSION);
		block.putStringValue(OpBlock.F_DETAILS, "");
		block.putStringValue(OpBlock.F_HASH, "");
		block.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.replicateBlock(block);
	}

	/**
	 * Expected ErrorType.OP_SIGNATURE_FAILED
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
	 * Expected ErrorType.OP_SIGNATURE_FAILED
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
	 *  Expected ErrorType.OP_HASH_IS_NOT_CORRECT
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
	 * Expected ErrorType.OP_SIZE_IS_EXCEEDED
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
	 * 	Expected ErrorType.OP_INVALID_VALIDATE_EXPRESSION
	 * 	@throws FailedVerificationException
	 */
	@Test
	public void testValidateRulesExpectError_OpInvalidValidateExpression() throws FailedVerificationException {
		String operation =
				"{  \n" +
				"\t\t\"type\" : \"sys.validate\",\n" +
				"\t\t\"new\" : [{ \n" +
				"\t\t\t\"id\" : [\"all_op_arity_new_del1\"],\n" +
				"\t\t\t\"type\" : [\"*\"],\n" +
				"\t\t\t\"comment\" : \"Validate operation arity\",\n" +
				"\t\t\t\"role\" : \"none\",\n" +
				"\t\t\t\"if\" : [\n" +
				"\t\t\t\t\"std:eq()\"\n" +
				"\t\t\t],\n" +
				"\t\t\t\"validate\" : [\n" +
				"\t\t\t\t\"std:leq(std:size(.new),1)\",\n" +
				"\t\t\t\t\"std:leq(std:size(.old),1)\"\n" +
				"\t\t\t]\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);

	}

	/**
	 *  Expected ErrorType.OP_ROLE_SUPER_ROLE_CIRCULAR_REF
	 *  @throws FailedVerificationException
	 */
	//TODO generate circular ref
	@Ignore
	@Test
	public void testValidateRulesExpectError_OpRoleSuperRoleCircularRef() throws FailedVerificationException {
		String operation =
				"{\n" +
				"\t\t\"type\" : \"sys.role\",\n" +
				"\t\t\"new\" : [{ \n" +
				"\t\t\t\"id\"\t: [\"owner\"],\n" +
				"\t\t\t\"comment\"   : \"Role master and only owner could change it\",\n" +
				"\t\t\t\"owner_role\" : \"master\",\n" +
				"\t\t\t\"super_roles\" : [\"master\"]\n" +
				"\t\t}]\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(operation);
		generateHashAndSignForOperation(opOperation, serverKeyPair, blc, true);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 *  Expected ErrorType.OP_ROLE_SUPER_ROLE_DOESNT_EXIST
	 *  @throws FailedVerificationException
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
	 *  Expected ErrorType.OP_GRANT_ROLE_DOESNT_EXIST
	 *  @throws FailedVerificationException
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

	private void selectOperations(List<OpOperation> opOperations, Set<String> operationsToDelete, Iterator<OpOperation> iterator) {
		int i = 0;

		while (i < 5) {
			OpOperation opOperation = iterator.next();
			opOperations.add(opOperation);
			operationsToDelete.add(opOperation.getRawHash());
			i++;
		}
	}
}