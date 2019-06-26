package org.openplacereviews.opendb.ops;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.*;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.*;
import static org.openplacereviews.opendb.ops.OpObject.F_COMMENT;
import static org.openplacereviews.opendb.ops.OpOperation.F_HASH;

public class OpBlockchainRulesTest {

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	private OpBlockChain blc;
	private JsonFormatter formatter;

	@Before
	public void beforeEachTestMethod() throws Exception {
		formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		generateOperations(formatter, blc);
	}

	/**
	 * Success adding new operation
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperation() throws FailedVerificationException {
		String id = "openplacereviews", role = "master", role1 = "administrator";

		OpObject opObject = new OpObject();
		opObject.setId(id + 1);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, id));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setType(OpBlockchainRules.OP_GRANT);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
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
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareReferencedObjectsExpectError_RefObjNotFound() throws
			FailedVerificationException {
		String name = "openplacereviews", name2 = "test_", role = "owner", secondRole = "administrator";

		OpObject opObject = new OpObject();
		opObject.setId(name + ":" + name2 + 2); //openplacereviews:test_2
		opObject.putStringValue(F_ROLES, role);
		opObject.addOrSetStringValue(F_ROLES, secondRole);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP + 1, name, name2 + 1));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setType(OpBlockchainRules.OP_GRANT);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expected ErrorType.DEL_OBJ_NOT_FOUND
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareDeletedObjectsExpectError_DelObjNotFound() throws FailedVerificationException {
		String name = "openplacereviews", notExistingHash =
				"fefffd95ccaa8b2545f2c5b8e1e7ae8c7d8f530b8d61be60df2345d74102c801";

		OpObject opObject = new OpObject();

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addDeleted(Collections.singletonList(notExistingHash));
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expected ErrorType.DEL_OBJ_DOUBLE_DELETED
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareDeletedObjectsExpectError_DelObjDoubleDeleted() throws
			FailedVerificationException {
		String name = "openplacereviews", oldHash = "fefffd95ccaa8b2545f2c5b8e1e7ae8c7d8f530b8d61be60df2345d74102c802";
		blc.createBlock(serverName, serverKeyPair);

		OpObject opObject = new OpObject();

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addDeleted(Collections.singletonList(oldHash));
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expected ErrorType.NEW_OBJ_DOUBLE_CREATED
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testAddOperationPrepareNoNewDuplicatedObjectsExpectError_NewObjDoubleCreated() throws
			FailedVerificationException {
		String name = "openplacereviews", name1 = "test_1", role = "master", secondRole = "administrator";

		OpObject opObject = new OpObject();
		opObject.setId(serverName);
		opObject.putStringValue(F_ROLES, role);
		opObject.addOrSetStringValue(F_ROLES, secondRole);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name, name1));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setType(OpBlockchainRules.OP_GRANT);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Success validation block
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlock() throws FailedVerificationException {
		assertNotNull(blc.createBlock(serverName, serverKeyPair));
	}

	/**
	 * Expected ErrorType.BLOCK_PREV_HASH
	 *
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
	 *
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
	 *
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
	 *
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
	 *
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
	 *
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
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateBlockExpectError_BlockSignatureFailed() throws FailedVerificationException {
		exceptionRule.expect(IllegalArgumentException.class);
		blc.createBlock(serverName + 1, serverKeyPair);
	}

	/**
	 * Expected ErrorType.BLOCK_HASH_FAILED
	 *
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
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateSignaturesExpectError_OpSignatureFailed() throws FailedVerificationException {
		String id = "openplacereviews", oauthMethod = "oauth";

		OpObject opObject = new OpObject();
		opObject.setId(id + 1);
		opObject.putStringValue(F_AUTH_METHOD, oauthMethod);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.remove(OpOperation.F_SIGNATURE);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.validateOperation(opOperation);
	}

	/**
	 * Expected ErrorType.OP_SIGNATURE_FAILED
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateSignaturesFirstSignupCouldBeSignedByItselfExpectError_OpSignatureFailed() throws
			FailedVerificationException {
		String id = "openplacereviews", oauthMethod = "oauth";

		OpObject opObject = new OpObject();
		opObject.setId(id + 1);
		opObject.putStringValue(F_AUTH_METHOD, oauthMethod);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		opOperation.setSignedBy(serverName + 1);
		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.remove(OpOperation.F_SIGNATURE);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.validateOperation(opOperation);
	}

	/**
	 * Success validateOperations
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateOp() throws FailedVerificationException {
		String id = "openplacereviews", oauthMethod = "oauth";

		OpObject opObject = new OpObject();
		opObject.setId(id + 1);
		opObject.putStringValue(F_AUTH_METHOD, oauthMethod);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		assertTrue(blc.validateOperation(opOperation));
	}

	/**
	 * Expected ErrorType.OP_HASH_IS_NOT_CORRECT
	 */
	@Test
	public void testValidateOpExpectError_OpHashIsNotCorrect() {
		String id = "openplacereviews", ownerRole = "administrator", superRole = "owner", opHash =
				"json:sha256:e55df720278460a277425e6331f08c436160a64db639051a82d79015e10dff03";

		OpObject opObject = new OpObject();
		opObject.setId(id);
		opObject.putStringValue(F_COMMENT, "some comment");
		opObject.putStringValue("owner_role", ownerRole);
		opObject.putStringValue("super_roles", superRole);

		OpOperation opOperation = new OpOperation();
		opOperation.putStringValue(F_HASH, opHash);
		opOperation.setType(OpBlockchainRules.OP_ROLE);
		opOperation.addCreated(opObject);

		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.validateOperation(opOperation);

	}

	/**
	 * Expected ErrorType.OP_SIZE_IS_EXCEEDED
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateOpExpectError_OpSizeIsExceeded() throws FailedVerificationException {
		OpOperation opOperation = formatter.parseOperation(generateBigJSON());
		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expected ErrorType.OP_INVALID_VALIDATE_EXPRESSION
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateRulesExpectError_OpInvalidValidateExpression() throws FailedVerificationException {
		String name = "all_op_arity_new_del1", comment = "Validate operation arity", role = "none";
		String ifStatement = "std:eq()", validateStatement1 = "std:leq(std:size(.new),1)", validateStatement2 =
				"std:leq(std:size(.old),1)";

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(F_TYPE, "*");
		opObject.putStringValue(F_COMMENT, comment);
		opObject.putStringValue(F_ROLES, role);
		opObject.putStringValue(F_IF, ifStatement);
		opObject.putStringValue(F_VALIDATE, validateStatement1);
		opObject.addOrSetStringValue(F_VALIDATE, validateStatement2);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_VALIDATE);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);

	}

	/**
	 * Expected ErrorType.OP_ROLE_SUPER_ROLE_DOESNT_EXIST
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateRulesExpectError_OpRoleSuperRoleDoesntExist() throws FailedVerificationException {
		String name = "owner1", ownerRole = "owner", superRole = "owner1";

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue("owner_role", ownerRole);
		opObject.putStringValue("super_roles", superRole);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_ROLE);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expected ErrorType.OP_GRANT_ROLE_DOESNT_EXIST
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testValidateRulesExpectError_OpGrantRoleDoesntExist() throws FailedVerificationException {
		String name = "openplacereviews", newRole = "owner1";

		OpObject opObject = new OpObject();
		opObject.setId(serverName + 1);
		opObject.putStringValue(F_ROLES, newRole);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setType(OpBlockchainRules.OP_GRANT);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(IllegalArgumentException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Test that merkle tree got the same result for same sequence,
	 * and different result if we change one single byte.
	 *
	 * @throws NoSuchMethodException
	 */
	@Test
	public void testCalculateMerkleTreeInPlaceHash() throws NoSuchMethodException, InvocationTargetException,
			IllegalAccessException {
		OpBlockchainRules bRules = blc.getRules();
		Class<OpBlockchainRules> clazz = OpBlockchainRules.class;
		Method calMerTreeM = clazz.getDeclaredMethod("calculateMerkleTreeInPlaceHash", String.class, List.class);
		calMerTreeM.setAccessible(true);

		List<byte[]> inChain = new ArrayList<>();
		for (int i = 0; i < 9; i++) {
			byte[] inData = new byte[200];
			new Random().nextBytes(inData);
			inChain.add(inData);
		}

		//have to get same sequence for the same bytes sequence
		String rootHash = (String) calMerTreeM.invoke(bRules, SecUtils.HASH_SHA256, inChain);
		String rootHashDuplicate = (String) calMerTreeM.invoke(bRules, SecUtils.HASH_SHA256, inChain);
		assertEquals(rootHash, rootHashDuplicate);

		//have to get different sequence for the different bytes sequence
		inChain.get(8)[198] = (byte) (inChain.get(8)[198] >> 1);
		String rootHash2 = (String) calMerTreeM.invoke(bRules, SecUtils.HASH_SHA256, inChain);
		assertNotEquals(rootHash, rootHash2);
	}

	private void selectOperations(List<OpOperation> opOperations, Set<String> operationsToDelete,
								  Iterator<OpOperation> iterator) {
		int i = 0;

		while (i < 5) {
			OpOperation opOperation = iterator.next();
			opOperations.add(opOperation);
			operationsToDelete.add(opOperation.getRawHash());
			i++;
		}
	}
}