package org.openplacereviews.opendb.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

import java.io.InputStreamReader;
import java.util.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.openplacereviews.opendb.api.MgmtController;
import org.openplacereviews.opendb.ops.OpBlockchainRules.BlockchainValidationException;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class OpBlockchainTest {

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public OpBlockChain blc;

	public JsonFormatter formatter;

	private Object[] parametersWithBlockchainAndBlock() throws FailedVerificationException {
		beforeEachTestMethod();

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		assertNotNull(opBlock);

		return new Object[]{
				blc
		};
	}

	private Object[] parametersWithBlockchain() throws FailedVerificationException {
		beforeEachTestMethod();

		return new Object[]{
				blc
		};
	}


	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		generateOperations(formatter, blc);
	}

	@Test
	@Parameters(method = "parametersWithBlockchainAndBlock")
	public void testOpBlockChain(OpBlockChain blcDB) {
		OpBlockChain opBlockChain = new OpBlockChain(blcDB.getParent(), blcDB.getRules());

		OpBlock opBlock = blcDB.getFullBlockByRawHash(blcDB.getBlockHeadersById(0).getRawHash());

		opBlockChain.replicateBlock(opBlock);
		blcDB.rebaseOperations(opBlockChain);

		assertTrue(opBlockChain.changeToEqualParent(opBlockChain.getParent()));

		OpBlockChain opBlockChain1 = new OpBlockChain(blcDB, opBlockChain, blcDB.getRules());
		assertNotNull(opBlockChain1);
	}

	@Test
	@Parameters(method = "parametersWithBlockchainAndBlock")
	public void testOpBlockChainWithNotEqualParents(OpBlockChain blcDB) {
		OpBlockChain opBlockChain = new OpBlockChain(blcDB.getParent(), blcDB.getRules());

		OpBlock opBlock = blcDB.getFullBlockByRawHash(blcDB.getBlockHeadersById(0).getRawHash());

		opBlockChain.replicateBlock(opBlock);

		exceptionRule.expect(IllegalStateException.class);
		new OpBlockChain(blcDB, opBlockChain, blcDB.getRules());
	}

	@Test
	public void testBlockChainStatus() {
		assertEquals(OpBlockChain.UNLOCKED, blc.getStatus());
	}

	@Test
	public void testValidationLocked() {
		blc.validateLocked();

		assertEquals(OpBlockChain.LOCKED_STATE, blc.getStatus());
	}

	@Test
	public void lockByUser() {
		blc.lockByUser();
		assertEquals(OpBlockChain.LOCKED_BY_USER, blc.getStatus());
	}

	@Test
	public void lockByUserIfBlockChainStatusIsLockedWithException() {
		blc.validateLocked();

		exceptionRule.expect(IllegalStateException.class);
		blc.lockByUser();
	}

	@Test
	public void unlockByUserIfBlockChainStatusLockedByUser() {
		blc.lockByUser();
		assertEquals(OpBlockChain.LOCKED_BY_USER, blc.getStatus());

		blc.unlockByUser();
		assertEquals(OpBlockChain.UNLOCKED, blc.getStatus());
	}

	@Test
	public void unlockByUserIfBlockChainStatusIsNotLockedByUserExpectException() {
		blc.validateLocked();
		assertEquals(OpBlockChain.LOCKED_STATE, blc.getStatus());

		exceptionRule.expect(IllegalStateException.class);
		blc.unlockByUser();
	}

	@Test
	@Parameters(method = "parametersWithBlockchain")
	public void testCreateBlock(OpBlockChain opBlockChain) throws FailedVerificationException {
		assertFalse(opBlockChain.getQueueOperations().isEmpty());
		assertEquals(-1, opBlockChain.getLastBlockId());

		assertNotNull(opBlockChain.createBlock(serverName, serverKeyPair));

		assertTrue(opBlockChain.getQueueOperations().isEmpty());
		assertEquals(0, opBlockChain.getLastBlockId());
	}

	@Test
	@Parameters(method = "parametersWithBlockchain")
	public void testCreateBlockWithLockedBlockChain(OpBlockChain opBlockChain) throws FailedVerificationException {
		assertFalse(opBlockChain.getQueueOperations().isEmpty());
		assertEquals(-1, opBlockChain.getLastBlockId());

		opBlockChain.validateLocked();
		assertEquals(OpBlockChain.LOCKED_STATE, opBlockChain.getStatus());

		exceptionRule.expect(IllegalStateException.class);
		opBlockChain.createBlock(serverName, serverKeyPair);
	}

	@Test
	public void testCreateBlockWithEmptyQueueWithException() throws FailedVerificationException {
		testRemoveAllQueueOperationsIfQueueNotEmpty();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.createBlock(serverName, serverKeyPair);
	}

	@Test
	public void testRemoveAllQueueOperationsIfQueueNotEmpty() {
		assertFalse(blc.getQueueOperations().isEmpty());

		blc.removeAllQueueOperations();

		assertTrue(blc.getQueueOperations().isEmpty());
	}

	@Test
	public void testRemoveAllQueueOperationsIfQueueIsEmpty() {
		assertFalse(blc.getQueueOperations().isEmpty());

		assertTrue(blc.removeAllQueueOperations());

		assertTrue(blc.getQueueOperations().isEmpty());

		assertTrue(blc.removeAllQueueOperations());
	}
	@Test
	public void testReplicateBlockWithNotImmutableOpBlock() throws FailedVerificationException {
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		opBlock.isImmutable = false;

		exceptionRule.expect(IllegalStateException.class);
		blc.replicateBlock(opBlock);
	}

	@Test
	public void testReplicateBlockWitLockedBlockChain() throws FailedVerificationException {
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		blc.validateLocked();
		assertEquals(OpBlockChain.LOCKED_STATE, blc.getStatus());

		exceptionRule.expect(IllegalStateException.class);
		blc.replicateBlock(opBlock);
	}

	@Test
	public void testReplicateBlockWithEmptyOperationQueue() throws FailedVerificationException {
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());

		assertNotNull(opBlockChain.replicateBlock(opBlock));

		assertEquals(0, opBlockChain.getLastBlockId());
	}

	@Test
	public void testReplicateBlockWithNotEmptyOperationQueue() throws FailedVerificationException {
		OpOperation opOperation = blc.getQueueOperations().removeFirst();

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());
		opBlockChain.addOperation(opOperation);

		assertNull(opBlockChain.replicateBlock(opBlock));
	}

	@Test
	@Parameters(method = "parametersWithBlockchainAndBlock")
	public void testRebaseOperations(OpBlockChain blcDB) {
		OpBlockChain opBlockChain1 = new OpBlockChain(blcDB.getParent(), blcDB.getRules());

		OpBlock opBlock = blcDB.getFullBlockByRawHash(blcDB.getBlockHeadersById(0).getRawHash());

		assertNotNull(opBlockChain1.replicateBlock(opBlock));
		assertTrue(blcDB.rebaseOperations(opBlockChain1));

		assertEquals(blcDB.getParent(), opBlockChain1);
	}

	@Test
	public void testRebaseOperationsWithNotEmptyOperationQueue() throws FailedVerificationException {
		OpOperation opOperation = blc.getQueueOperations().removeFirst();
		blc.createBlock(serverName, serverKeyPair);

		OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());
		opBlockChain.addOperation(opOperation);

		assertFalse(blc.rebaseOperations(opBlockChain));
	}

	@Test
	@Parameters(method = "parametersWithBlockchainAndBlock")
	public void testChangeToEqualParent(OpBlockChain blcDB) {
		OpBlockChain opBlockChain1 = new OpBlockChain(blcDB.getParent(), blcDB.getRules());

		OpBlock opBlock = blcDB.getFullBlockByRawHash(blcDB.getBlockHeadersById(0).getRawHash());
		opBlockChain1.replicateBlock(opBlock);

		blcDB.rebaseOperations(opBlockChain1);

		assertTrue(opBlockChain1.changeToEqualParent(opBlockChain1.getParent()));
	}

	@Test
	public void testChangeToEqualLockedParent() {
		OpBlockChain newOp = new OpBlockChain(OpBlockChain.NULL, blc.getRules());
		newOp.lockByUser();
		exceptionRule.expect(IllegalStateException.class);
		blc.changeToEqualParent(newOp);
	}

	@Test
	public void testChangeToNotEqualParent() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);
		OpBlockChain opBlockChain = new OpBlockChain(OpBlockChain.NULL, blc.getRules());

		assertFalse(opBlockChain.changeToEqualParent(blc));
	}

	@Test
	public void testAddOperations() {
		OpOperation loadedOperation = blc.getQueueOperations().getLast();
		OpOperation opOperation = new OpOperation(loadedOperation, true);
		opOperation.makeImmutable();

		int amountLoadedOperations = blc.getQueueOperations().size();

		OpBlockChain blockchain = new OpBlockChain(blc.getParent(), blc.getRules());
		for (OpOperation o : blc.getQueueOperations()) {
			if (!o.getRawHash().equals(loadedOperation.getRawHash())) {
				blockchain.addOperation(o);
			}
		}
		blc = blockchain;

		assertEquals(amountLoadedOperations - 1, blc.getQueueOperations().size());
		assertTrue(blc.addOperation(opOperation));
	}

	@Test
	public void testAddOperationsIfOperationIsAlreadyExists() {
		OpOperation loadedOperation = blc.getQueueOperations().getLast();
		OpOperation opOperation = new OpOperation(loadedOperation, true);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	@Test
	public void testAddOperationsWhenOperationIsMutable() {
		OpOperation opOperation = new OpOperation();

		exceptionRule.expect(IllegalStateException.class);
		blc.addOperation(opOperation);
	}

	@Test
	public void testAddOperationsWhenBlockChainIsLocked() {
		OpOperation opOperation = new OpOperation();
		opOperation.makeImmutable();

		blc.validateLocked(); // LOCKED state

		exceptionRule.expect(IllegalStateException.class);
		blc.addOperation(opOperation);
	}

	@Test
	public void testValidateOperations() {
		OpOperation loadedOperation = blc.getQueueOperations().getLast();
		OpOperation opOperation = new OpOperation(loadedOperation, true);
		opOperation.makeImmutable();

		int amountLoadedOperations = blc.getQueueOperations().size();

		OpBlockChain blockchain = new OpBlockChain(blc.getParent(), blc.getRules());
		for (OpOperation o : blc.getQueueOperations()) {
			if (!o.getRawHash().equals(loadedOperation.getRawHash())) {
				blockchain.addOperation(o);
			}
		}
		blc = blockchain;
		assertEquals(amountLoadedOperations - 1, blc.getQueueOperations().size());

		assertTrue(blc.validateOperation(opOperation));
	}

	@Test
	public void testValidateOperationsIfOperationIsAlreadyExists() {
		OpOperation loadedOperation = blc.getQueueOperations().getLast();
		OpOperation opOperation = new OpOperation(loadedOperation, true);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.validateOperation(opOperation);
	}

	@Test
	public void testValidateOperationsWhenOperationIsMutable() {
		OpOperation opOperation = new OpOperation();

		exceptionRule.expect(IllegalStateException.class);
		blc.validateOperation(opOperation);
	}

	@Test
	public void testValidateOperationsWhenBlockChainIsLocked() {
		OpOperation opOperation = new OpOperation();
		opOperation.makeImmutable();

		blc.validateLocked(); // LOCKED state

		exceptionRule.expect(IllegalStateException.class);
		blc.validateOperation(opOperation);
	}


	@Test
	public void testCreatingACopyOfObject() {
		JsonFormatter formatter = new JsonFormatter();
		String msg = "{\n" +
				"\t\t\"type\" : \"sys.signup\",\n" +
				"\t\t\"signed_by\": \"openplacereviews\",\n" +
				"\t\t\"create\": [{\n" +
				"\t\t\t\"id\": [\"openplacereviews\"],\n" +
				"\t\t\t\"name\" : \"openplacereviews\",\n" +
				"\t\t\t\"algo\": \"EC\",\n" +
				"\t\t\t\"auth_method\": \"provided\",\n" +
				"\t\t\t\"pubkey\": \"base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAETxKWXg3jhSxBtYhTxO/zMj6S0jO95ETUehlZ7yR150gFSmxqJcLE4YQDZ6t/Hn13kmaZbhSFMNsAX+kbeUMqbQ==\"\n" +
				"\t\t}],\n" +
				"\t\t\"hash\": \"json:sha256:439715024c856dd93b4bf45bb0c592621d1f6c0905078a4e73cf73d400ff2a1b\",\n" +
				"\t\t\"signature\": \"ECDSA:base64:MEYCIQDa+wCjXGRxUsTDUMHCTX8kwofiSvlfK2IqjfO7mP5wjAIhAI4x4T1X43qhpTpmcHxHZo2bgHw1mGiCGXeX4NjJBaDB\"\n" +
				"\t}";

		OpOperation opOperation = formatter.parseOperation(msg);

		OpObject old = opOperation.getCreated().get(0);
		OpObject copy = new OpObject(old);

		copy.isImmutable = false;
		copy.setId("test1");
		assertFalse(copy.isImmutable());
		assertNotEquals(copy.getId(), old.getId());
	}

	@Test
	public void testCreateAndEditObject() throws FailedVerificationException {
		JsonFormatter formatter = new JsonFormatter();
		generateOperationsByList(formatter, blc, MULTIPLE_DELETE_LIST);
		OpObject opObject = blc.getObjectByName("osm.place","8FW97P", "wdhpik");
		int countEl = ((List)opObject.getStringListObjMap("images").get("review")).size();

		assertEquals( 3, countEl);
	}

	private OpObject generateTestOpObject() {
		OpObject opObject = new OpObject();
		opObject.setId("some id");
		opObject.putObjectValue("osmId", Arrays.asList("3erfsdfsdfs", "34234232"));
		opObject.putObjectValue("lon", 12345);
		opObject.putObjectValue("lat", "222EC");
		Map<String, Object> tagsMap = new HashMap<>();
		tagsMap.put("v", 11111111);
		Map<String, Object> tagsklMap = new HashMap<>();
		tagsklMap.put("l", Arrays.asList("2343","434r4232"));
		tagsklMap.put("t", 22222222);
		tagsMap.put("k", tagsklMap);
		opObject.putObjectValue("tags", Collections.singletonList(tagsMap));
		return opObject;
	}


}
