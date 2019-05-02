package org.openplacereviews.opendb.ops;

import org.junit.Before;
import org.junit.Test;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.ops.de.OperationDeleteInfo;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.security.KeyPair;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateOperations;
import static org.openplacereviews.opendb.VariableHelperTest.*;

public class OpBlockchainGettersTest {

	private OpBlockChain blc;
	private KeyPair serverKeyPair;

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		JsonFormatter formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		this.serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverKey, serverPublicKey);
		generateOperations(formatter, blc, serverKeyPair);
	}


	@Test
	public void testGetLastBlockFullHashIfBlockExist() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		assertNotNull(blc.getLastBlockFullHash());
	}

	@Test
	public void testGetLastBlockFullHashIfBlockIsNotExist() {
		assertEquals("", blc.getLastBlockFullHash());
	}

	@Test
	public void testGetLastBlockRawHashHashIfBlockExist() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		assertNotNull(blc.getLastBlockRawHash());
	}

	@Test
	public void testGetLastBlockRawHashIfBlockIsNotExist() {
		assertEquals("", blc.getLastBlockRawHash());
	}

	@Test
	public void testGetSuperBlockHash() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		assertNotNull(blc.getSuperBlockHash());
	}

	@Test
	public void testGetSuperBlockHashIfSuperBlockWasNotCreated() {
		assertEquals("", blc.getSuperBlockHash());
	}

	@Test
	public void testGetSuperBlockSize() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		assertEquals(1, blc.getSuperblockSize());
	}

	@Test
	public void testGetSuperblockHeadersIfBlockWasNotCreated() {
		Deque<OpBlock> opBlockDeque = blc.getSuperblockHeaders();

		assertTrue(opBlockDeque.isEmpty());
	}

	@Test
	public void testGetSuperblockHeaders() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		Deque<OpBlock> opBlockDeque = blc.getSuperblockHeaders();

		assertFalse(opBlockDeque.isEmpty());
	}

	@Test
	public void testGetSuperblockFullBlocksIfBlockWasNotCreated() {
		Deque<OpBlock> opBlockDeque = blc.getSuperblockFullBlocks();

		assertFalse(blc.isDbAccessed());
		assertTrue(opBlockDeque.isEmpty());
	}

	@Test
	public void testGetSuperblockFullBlocks() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);
		Deque<OpBlock> opBlockDeque = blc.getSuperblockFullBlocks();

		assertFalse(blc.isDbAccessed());
		assertFalse(opBlockDeque.isEmpty());
	}

	@Test
	public void testGetSuperblockDeleteInfo() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);

		Collection<OperationDeleteInfo> listOperationDeleteInfo = blc.getSuperblockDeleteInfo();
		assertFalse(listOperationDeleteInfo.isEmpty());
	}

	@Test
	public void testGetSuperblockObjects () {
		final int amountLoadedObjects = 6;
		Map<String, Map<CompoundKey, OpObject>> superBlockObject = blc.getSuperblockObjects();

		assertEquals(amountLoadedObjects, superBlockObject.size());
	}

	@Test
	public void testGetBlockHeaders() throws FailedVerificationException {
		blc.createBlock(serverName, serverKeyPair);
		int depth = blc.getDepth();

		List<OpBlock> blockHeaders = blc.getBlockHeaders(depth);

		assertFalse(blockHeaders.isEmpty());
	}

	@Test
	public void testGetBlockHeadersById() throws FailedVerificationException {
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		int lastBlockId = blc.getLastBlockId();
		OpBlock loadedOpBlock = blc.getBlockHeadersById(lastBlockId);
		assertNotNull(loadedOpBlock);

		assertEquals(opBlock.getRawHash(), loadedOpBlock.getRawHash());
	}

	@Test
	public void testGetBlockHeadersByNotExistingId() {
		final int notExistingId = 0;

		assertNull(blc.getBlockHeadersById(notExistingId));
	}

	@Test
	public void testGetBlockHeaderByRawHash() throws FailedVerificationException {
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlock loadedOpBlock = blc.getBlockHeaderByRawHash(opBlock.getRawHash());

		assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
	}

	@Test
	public void testGetBlockHeaderByNotExistingRawHash() {
		OpBlock loadedOpBlock = blc.getBlockHeaderByRawHash("1");
		assertNull(loadedOpBlock);
	}

	@Test
	public void testGetFullBlockByRawHash() throws FailedVerificationException {
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlock loadedOpBlock = blc.getFullBlockByRawHash(opBlock.getRawHash());

		assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
	}

	@Test
	public void testGetFullBlockByNotExistingRawHash() {
		OpBlock loadedOpBlock = blc.getFullBlockByRawHash("1");
		assertNull(loadedOpBlock);
	}

	@Test
	public void testGetOperationByHash() {
		OpOperation queueOperation = blc.getQueueOperations().getLast();

		OpOperation opOperation = blc.getOperationByHash(queueOperation.getRawHash());

		assertEquals(queueOperation, opOperation);
	}

	@Test
	public void testGetOperationByNotExistingHash() {
		OpOperation opOperation = blc.getOperationByHash("1");

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

		assertEquals(1, r.result.size());
	}
}
