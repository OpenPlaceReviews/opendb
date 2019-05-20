package org.openplacereviews.opendb.ops;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.ops.de.OperationDeleteInfo;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

@RunWith(JUnitParamsRunner.class)
public class OpBlockchainGettersTest {

	public OpBlockChain blc;

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		JsonFormatter formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		generateOperations(formatter, blc);
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetLastBlockFullHashIfBlockExist(OpBlockChain opBlockChain) {
		assertNotNull(opBlockChain.getLastBlockFullHash());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetLastBlockFullHashIfBlockIsNotExist(OpBlockChain opBlockChain) {
		assertEquals("", opBlockChain.getLastBlockFullHash());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetLastBlockRawHashHashIfBlockExist(OpBlockChain opBlockChain) {
		assertNotNull(opBlockChain.getLastBlockRawHash());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetLastBlockRawHashIfBlockIsNotExist(OpBlockChain opBlockChain) {
		assertEquals("", opBlockChain.getLastBlockRawHash());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetSuperBlockHash(OpBlockChain opBlockChain) {
		assertNotNull(opBlockChain.getSuperBlockHash());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetSuperBlockHashIfSuperBlockWasNotCreated(OpBlockChain opBlockChain) {
		assertEquals("", opBlockChain.getSuperBlockHash());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetSuperBlockSize(OpBlockChain opBlockChain) {
		assertEquals(1, opBlockChain.getSuperblockSize());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetSuperblockHeadersIfBlockWasNotCreated(OpBlockChain opBlockChain) {
		Deque<OpBlock> opBlockDeque = opBlockChain.getSuperblockHeaders();

		assertTrue(opBlockDeque.isEmpty());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetSuperblockHeaders(OpBlockChain opBlockChain) {
		Deque<OpBlock> opBlockDeque = opBlockChain.getSuperblockHeaders();

		assertFalse(opBlockDeque.isEmpty());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetSuperblockFullBlocksIfBlockWasNotCreated(OpBlockChain opBlockChain) {
		Deque<OpBlock> opBlockDeque = opBlockChain.getSuperblockFullBlocks();

		assertTrue(opBlockDeque.isEmpty());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetSuperblockFullBlocks(OpBlockChain opBlockChain) {
		Deque<OpBlock> opBlockDeque = opBlockChain.getSuperblockFullBlocks();

		assertFalse(opBlockDeque.isEmpty());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetSuperblockDeleteInfo(OpBlockChain opBlockChain) {
		Collection<OperationDeleteInfo> listOperationDeleteInfo = opBlockChain.getSuperblockDeleteInfo();
		assertFalse(listOperationDeleteInfo.isEmpty());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetSuperblockObjects(OpBlockChain opBlockChain) {
		final int amountLoadedObjects = 6;
		Map<String, Map<CompoundKey, OpObject>> superBlockObject = opBlockChain.getSuperblockObjects();

		assertEquals(amountLoadedObjects, superBlockObject.size());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetBlockHeaders(OpBlockChain opBlockChain) {
		int depth = opBlockChain.getDepth();

		List<OpBlock> blockHeaders = opBlockChain.getBlockHeaders(depth);

		assertFalse(blockHeaders.isEmpty());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetBlockHeadersById(OpBlockChain opBlockChain) {
		int lastBlockId = opBlockChain.getLastBlockId();
		OpBlock opBlock = opBlockChain.getBlockHeadersById(0);
		OpBlock loadedOpBlock = opBlockChain.getBlockHeadersById(lastBlockId);
		assertNotNull(loadedOpBlock);

		assertEquals(opBlock.getRawHash(), loadedOpBlock.getRawHash());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetBlockHeadersByNotExistingId(OpBlockChain opBlockChain) {
		final int notExistingId = 0;

		assertNull(opBlockChain.getBlockHeadersById(notExistingId));
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetBlockHeaderByRawHash(OpBlockChain opBlockChain) {
		OpBlock opBlock = opBlockChain.getBlockHeadersById(0);
		OpBlock loadedOpBlock = opBlockChain.getBlockHeaderByRawHash(opBlock.getRawHash());

		assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetBlockHeaderByNotExistingRawHash(OpBlockChain opBlockChain) {
		OpBlock loadedOpBlock = opBlockChain.getBlockHeaderByRawHash("1");
		assertNull(loadedOpBlock);
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithBlock")
	public void testGetFullBlockByRawHash(OpBlockChain opBlockChain) {
		OpBlock opBlock = opBlockChain.getBlockHeadersById(0);
		OpBlock loadedOpBlock = opBlockChain.getFullBlockByRawHash(opBlock.getRawHash());

		assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithoutBlock")
	public void testGetFullBlockByNotExistingRawHash(OpBlockChain opBlockChain) {
		OpBlock loadedOpBlock = opBlockChain.getFullBlockByRawHash("1");
		assertNull(loadedOpBlock);
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithExistingOperationRowHash")
	public void testGetOperationByHash(OpBlockChain opBlockChain, String operation) {
		OpOperation opOperation = opBlockChain.getOperationByHash(operation);

		assertNotNull(opOperation);
	}

	@Test
	@Parameters(method = "opblockchainBasicParameterWithNotExistingOperationRowHash")
	public void testGetOperationByNotExistingHash(OpBlockChain opBlockChain, String operation) {
		OpOperation opOperation = opBlockChain.getOperationByHash(operation);

		assertNull(opOperation);
	}

	@Test
	public void testSimpleFunctionEval() {
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		r.limit = 100;

		blc.getObjects(OpBlockchainRules.OP_OPERATION, r);

		assertEquals(6, r.result.size());
	}

	@Test
	public void testSimpleFunctionEval2() {
		OpBlockChain.ObjectsSearchRequest r = new OpBlockChain.ObjectsSearchRequest();
		r.limit = 100;

		blc.getObjects(OpBlockchainRules.OP_SIGNUP, r);

		assertEquals(2, r.result.size());
	}

	private Object[] opblockchainBasicParameterWithBlock() throws FailedVerificationException {
		beforeEachTestMethod();

		blc.createBlock(serverName, serverKeyPair);
		return new Object[]{
				blc
		};
	}

	private Object[] opblockchainBasicParameterWithoutBlock() throws FailedVerificationException {
		beforeEachTestMethod();

		return new Object[]{
				blc
		};
	}

	private Object[] opblockchainBasicParameterWithExistingOperationRowHash() throws FailedVerificationException {
		beforeEachTestMethod();

		return new Object[]{
				blc, blc.getQueueOperations().getLast().getRawHash()
		};
	}

	private Object[] opblockchainBasicParameterWithNotExistingOperationRowHash() throws FailedVerificationException {
		beforeEachTestMethod();

		return new Object[]{
				blc, "1"
		};
	}
}
