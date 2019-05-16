package org.openplacereviews.opendb.ops;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openplacereviews.opendb.FailedVerificationException;
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

	private Object[] parametersWithNullableBlockchain() {
		return new Object[] {
				null
		};
	}

	private Object[] parametersWithNullableBlockchainAndOpObject() {
		return new Object[] {
				null, null
		};
	}

	public OpBlockChain blc;

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		JsonFormatter formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		generateOperations(formatter, blc);
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetLastBlockFullHashIfBlockExist(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			assertNotNull(opBlockChain.getLastBlockFullHash());
		} else {
			blc.createBlock(serverName, serverKeyPair);

			assertNotNull(blc.getLastBlockFullHash());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetLastBlockFullHashIfBlockIsNotExist(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			assertEquals("", opBlockChain.getLastBlockFullHash());
		} else {
			assertEquals("", blc.getLastBlockFullHash());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetLastBlockRawHashHashIfBlockExist(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			assertNotNull(opBlockChain.getLastBlockRawHash());
		} else {
			blc.createBlock(serverName, serverKeyPair);

			assertNotNull(blc.getLastBlockRawHash());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetLastBlockRawHashIfBlockIsNotExist(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			assertEquals("", opBlockChain.getLastBlockRawHash());
		} else {

		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperBlockHash(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			assertNotNull(opBlockChain.getSuperBlockHash());
		} else {
			blc.createBlock(serverName, serverKeyPair);

			assertNotNull(blc.getSuperBlockHash());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperBlockHashIfSuperBlockWasNotCreated(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			assertEquals("", opBlockChain.getSuperBlockHash());
		} else {
			assertEquals("", blc.getSuperBlockHash());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperBlockSize(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			assertEquals(1, opBlockChain.getSuperblockSize());
		} else {
			blc.createBlock(serverName, serverKeyPair);

			assertEquals(1, blc.getSuperblockSize());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperblockHeadersIfBlockWasNotCreated(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			Deque<OpBlock> opBlockDeque = blc.getSuperblockHeaders();

			assertTrue(opBlockDeque.isEmpty());
		} else {
			Deque<OpBlock> opBlockDeque = blc.getSuperblockHeaders();

			assertTrue(opBlockDeque.isEmpty());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperblockHeaders(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			Deque<OpBlock> opBlockDeque = opBlockChain.getSuperblockHeaders();

			assertFalse(opBlockDeque.isEmpty());
		} else {
			blc.createBlock(serverName, serverKeyPair);

			Deque<OpBlock> opBlockDeque = blc.getSuperblockHeaders();

			assertFalse(opBlockDeque.isEmpty());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperblockFullBlocksIfBlockWasNotCreated(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			Deque<OpBlock> opBlockDeque = opBlockChain.getSuperblockFullBlocks();

			assertTrue(opBlockChain.isDbAccessed());
			assertTrue(opBlockDeque.isEmpty());
		} else {
			Deque<OpBlock> opBlockDeque = blc.getSuperblockFullBlocks();

			assertFalse(blc.isDbAccessed());
			assertTrue(opBlockDeque.isEmpty());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperblockFullBlocks(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			Deque<OpBlock> opBlockDeque = opBlockChain.getSuperblockFullBlocks();

			assertTrue(opBlockChain.isDbAccessed());
			assertFalse(opBlockDeque.isEmpty());
		} else {
			blc.createBlock(serverName, serverKeyPair);
			Deque<OpBlock> opBlockDeque = blc.getSuperblockFullBlocks();

			assertFalse(blc.isDbAccessed());
			assertFalse(opBlockDeque.isEmpty());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperblockDeleteInfo(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			Collection<OperationDeleteInfo> listOperationDeleteInfo = opBlockChain.getSuperblockDeleteInfo();
			assertFalse(listOperationDeleteInfo.isEmpty());
		} else {
			blc.createBlock(serverName, serverKeyPair);

			Collection<OperationDeleteInfo> listOperationDeleteInfo = blc.getSuperblockDeleteInfo();
			assertFalse(listOperationDeleteInfo.isEmpty());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetSuperblockObjects(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			final int amountLoadedObjects = 6;
			Map<String, Map<CompoundKey, OpObject>> superBlockObject = opBlockChain.getSuperblockObjects();

			assertEquals(amountLoadedObjects, superBlockObject.size());
		} else {
			final int amountLoadedObjects = 6;
			Map<String, Map<CompoundKey, OpObject>> superBlockObject = blc.getSuperblockObjects();

			assertEquals(amountLoadedObjects, superBlockObject.size());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetBlockHeaders(OpBlockChain opBlockChain) throws FailedVerificationException {
		if (opBlockChain != null) {
			int depth = opBlockChain.getDepth();

			List<OpBlock> blockHeaders = opBlockChain.getBlockHeaders(depth);

			assertFalse(blockHeaders.isEmpty());
		} else {
			blc.createBlock(serverName, serverKeyPair);
			int depth = blc.getDepth();

			List<OpBlock> blockHeaders = blc.getBlockHeaders(depth);

			assertFalse(blockHeaders.isEmpty());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchainAndOpObject")
	public void testGetBlockHeadersById(OpBlockChain opBlockChain, OpBlock opBlock) throws FailedVerificationException {
		if (opBlockChain != null) {
			int lastBlockId = opBlockChain.getLastBlockId();
			OpBlock loadedOpBlock = opBlockChain.getBlockHeadersById(lastBlockId);
			assertNotNull(loadedOpBlock);

			assertEquals(opBlock.getRawHash(), loadedOpBlock.getRawHash());
		} else {
			opBlock = blc.createBlock(serverName, serverKeyPair);

			int lastBlockId = blc.getLastBlockId();
			OpBlock loadedOpBlock = blc.getBlockHeadersById(lastBlockId);
			assertNotNull(loadedOpBlock);

			assertEquals(opBlock.getRawHash(), loadedOpBlock.getRawHash());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetBlockHeadersByNotExistingId(OpBlockChain opBlockChain) {
		final int notExistingId = 0;

		if (opBlockChain != null) {
			assertNull(opBlockChain.getBlockHeadersById(notExistingId));
		} else {
			assertNull(blc.getBlockHeadersById(notExistingId));
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchainAndOpObject")
	public void testGetBlockHeaderByRawHash(OpBlockChain opBlockChain, OpBlock opBlock) throws FailedVerificationException {
		if (opBlockChain != null) {
			OpBlock loadedOpBlock = opBlockChain.getBlockHeaderByRawHash(opBlock.getRawHash());

			assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
		} else {
			opBlock = blc.createBlock(serverName, serverKeyPair);

			OpBlock loadedOpBlock = blc.getBlockHeaderByRawHash(opBlock.getRawHash());

			assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetBlockHeaderByNotExistingRawHash(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			OpBlock loadedOpBlock = opBlockChain.getBlockHeaderByRawHash("1");
			assertNull(loadedOpBlock);
		} else {
			OpBlock loadedOpBlock = blc.getBlockHeaderByRawHash("1");
			assertNull(loadedOpBlock);
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchainAndOpObject")
	public void testGetFullBlockByRawHash(OpBlockChain opBlockChain, OpBlock opBlock) throws FailedVerificationException {
		if (opBlockChain != null) {
			OpBlock loadedOpBlock = blc.getFullBlockByRawHash(opBlock.getRawHash());

			assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
		} else {
			opBlock = blc.createBlock(serverName, serverKeyPair);

			OpBlock loadedOpBlock = blc.getFullBlockByRawHash(opBlock.getRawHash());

			assertEquals(opBlock.getBlockId(), loadedOpBlock.getBlockId());
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetFullBlockByNotExistingRawHash(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			OpBlock loadedOpBlock = opBlockChain.getFullBlockByRawHash("1");
			assertNull(loadedOpBlock);
		} else {
			OpBlock loadedOpBlock = blc.getFullBlockByRawHash("1");
			assertNull(loadedOpBlock);
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchainAndOpObject")
	public void testGetOperationByHash(OpBlockChain opBlockChain, OpOperation operation) {
		if (opBlockChain != null) {
			OpOperation opOperation = opBlockChain.getOperationByHash(operation.getRawHash());

			assertEquals(operation, opOperation);
		} else {
			OpOperation queueOperation = blc.getQueueOperations().getLast();

			OpOperation opOperation = blc.getOperationByHash(queueOperation.getRawHash());

			assertEquals(queueOperation, opOperation);
		}
	}

	@Test
	@Parameters(method = "parametersWithNullableBlockchain")
	public void testGetOperationByNotExistingHash(OpBlockChain opBlockChain) {
		if (opBlockChain != null) {
			OpOperation opOperation = opBlockChain.getOperationByHash("10c5978d2466b67505d2d94a9a0f29695e03bf11893a4a5cac3cd700aa757dd9");

			assertNull(opOperation);
		} else {
			OpOperation opOperation = blc.getOperationByHash("1");

			assertNull(opOperation);
		}
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
}
