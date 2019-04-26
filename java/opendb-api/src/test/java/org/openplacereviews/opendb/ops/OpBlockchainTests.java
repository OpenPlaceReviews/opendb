package org.openplacereviews.opendb.ops;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.MgmtController;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.ops.de.OperationDeleteInfo;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.io.InputStreamReader;
import java.security.KeyPair;
import java.util.*;

import static org.junit.Assert.*;

public class OpBlockchainTests {

	public static final String SIMPLE_JSON = "{'a':1, 'b': 'b', 'c' : ['1', '2'], 'e' : {'a': {'a':3}} }";
	String serverName = "openplacereviews:test_1";
	String serverKey = "base64:PKCS#8:MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCAOpUDyGrTPRPDQRCIRXysxC6gCgSTiNQ5nVEjhvsFITA==";
	String serverPublicKey = "base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAETxKWXg3jhSxBtYhTxO/zMj6S0jO95ETUehlZ7yR150gFSmxqJcLE4YQDZ6t/Hn13kmaZbhSFMNsAX+kbeUMqbQ==";
	
	protected String[] BOOTSTRAP_LIST = 
			new String[] {"opr-0-test-user", "std-ops-defintions", "std-roles", "std-validations", "opr-0-test-grant"};
	
	private OpBlockChain blc;
	private JsonFormatter formatter;
	private KeyPair serverKeyPair;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();
	
	@BeforeClass
    public static void beforeAllTestMethods() {
    }
 
    @Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		this.serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverKey, serverPublicKey);
		for (String f : BOOTSTRAP_LIST) {
			OpOperation[] lst = formatter.fromJson(
					new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/" + f + ".json")),
					OpOperation[].class);
			for (OpOperation o : lst) {
				if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
					o.setSignedBy(serverName);
					o = blc.getRules().generateHashAndSign(o, serverKeyPair);
				}
				o.makeImmutable();
				blc.addOperation(o);
			}
		}
	}

	@Test
    public void testOpBlockChain() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());

        opBlockChain.replicateBlock(opBlock);
        blc.rebaseOperations(opBlockChain);

        assertTrue(opBlockChain.changeToEqualParent(opBlockChain.getParent()));

        OpBlockChain opBlockChain1 = new OpBlockChain(blc, opBlockChain, blc.getRules());
        assertNotNull(opBlockChain1);
    }

    @Test
    public void testOpBlockChainWithNotEqualParents() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());

        opBlockChain.replicateBlock(opBlock);

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Wrong parameters to create object with merged parents");
        new OpBlockChain(blc, opBlockChain, blc.getRules());
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
	    exceptionRule.expectMessage("This chain is locked not by user or in a broken state");
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
        exceptionRule.expectMessage("This chain is locked not by user or in a broken state");
        blc.unlockByUser();
    }

    @Test
    public void testCreateBlock() throws FailedVerificationException {
        assertFalse(blc.getQueueOperations().isEmpty());
        assertEquals(-1, blc.getLastBlockId());

	    assertNotNull(blc.createBlock(serverName, serverKeyPair));

        assertTrue(blc.getQueueOperations().isEmpty());
        assertEquals(0, blc.getLastBlockId());
    }

    @Test
    public void testCreateBlockWithLockedBlockChain() throws FailedVerificationException {
        assertFalse(blc.getQueueOperations().isEmpty());
        assertEquals(-1, blc.getLastBlockId());

        blc.validateLocked();
        assertEquals(OpBlockChain.LOCKED_STATE, blc.getStatus());

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("This chain is immutable");
        blc.createBlock(serverName, serverKeyPair);
    }

    @Test
    public void testCreateBlockWithEmptyQueueWithException() throws FailedVerificationException {
	    testRemoveAllQueueOperationsIfQueueNotEmpty();

        exceptionRule.expect(IllegalArgumentException.class);
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
    public void testRemoveQueueOperationsByListOfRowHashes() {
	    final int amountOperationsForRemoving = 5;

        Deque<OpOperation> dequeOperations = blc.getQueueOperations();
        assertFalse(dequeOperations.isEmpty());

        int i = 0;
        Set<String> operationsToDelete = new HashSet<>();

        Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
        while (i < amountOperationsForRemoving) {
            operationsToDelete.add(iterator.next().getRawHash());

            i++;
        }

        Set<String> removedOperations = blc.removeQueueOperations(operationsToDelete);
        assertEquals(amountOperationsForRemoving, removedOperations.size());
    }

    @Test
    public void testRemoveQueueOperationsByEmptyListOfHashes() {
        assertFalse(blc.getQueueOperations().isEmpty());

        Set<String> operationsToDelete = new HashSet<>();

        assertTrue(blc.removeQueueOperations(operationsToDelete).isEmpty());
    }

    @Test
    public void testRemoveQueueOperationsByNotExistingHash() {
        assertFalse(blc.getQueueOperations().isEmpty());

        Set<String> operationsToDelete = new HashSet<>();
        operationsToDelete.add(UUID.randomUUID().toString());

        assertTrue(blc.removeQueueOperations(operationsToDelete).isEmpty());
    }

    @Test
    public void testReplicateBlockWithNotImmutableOpBlock() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        opBlock.isImmutable = false;

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Object is mutable");
        blc.replicateBlock(opBlock);
    }

    @Test
    public void testReplicateBlockWitLockedBlockChain() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        blc.validateLocked();
        assertEquals(OpBlockChain.LOCKED_STATE, blc.getStatus());

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("This chain is immutable");
        blc.replicateBlock(opBlock);
    }

    @Test
    public void testReplicateBlockWithEmptyOperationQueue() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());

        assertNotNull(opBlockChain.replicateBlock(opBlock));

        assertEquals(0, opBlockChain.getLastBlockId());
    }


    //TODO add operations to queue
    @Ignore
    @Test
    public void testReplicateBlockWithNotEmptyOperationQueue() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());



        assertNull(opBlockChain.replicateBlock(opBlock));
    }

    @Test
    public void testRebaseOperations() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());

        assertNotNull(opBlockChain.replicateBlock(opBlock));
        assertTrue(blc.rebaseOperations(opBlockChain));

        assertEquals(blc.getParent(), opBlockChain);
    }

    //TODO add operations to queue
    @Ignore
    @Test
    public void testRebaseOperationsWithNotEmptyOperationQueue() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());

        blc.rebaseOperations(opBlockChain);
    }


    @Test
    public void testChangeToEqualParent() throws FailedVerificationException {
        OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

        OpBlockChain opBlockChain = new OpBlockChain(blc.getParent(), blc.getRules());

        opBlockChain.replicateBlock(opBlock);
        blc.rebaseOperations(opBlockChain);

        assertTrue(opBlockChain.changeToEqualParent(opBlockChain.getParent()));
    }

    //TODO generate Locked opBlockChain
    @Ignore
    @Test
    public void testChangeToEqualLockedParent() {
    }

    //TODO generate other opBlockChain
    @Ignore
    @Test
    public void testChangeToNotEqualParent() {
    }

    @Test
    public void testAddOperations() {
	    OpOperation loadedOperation = blc.getQueueOperations().getLast();
	    OpOperation opOperation = new OpOperation(loadedOperation, true);
	    opOperation.makeImmutable();

	    int amountLoadedOperations = blc.getQueueOperations().size();
        blc.removeQueueOperations(new HashSet<>(Collections.singletonList(loadedOperation.getRawHash())));
        assertEquals(amountLoadedOperations - 1, blc.getQueueOperations().size());

	    assertTrue(blc.addOperation(opOperation));
    }

    @Test
    public void testAddOperationsIfOperationIsAlreadyExists() {
        OpOperation loadedOperation = blc.getQueueOperations().getLast();
        OpOperation opOperation = new OpOperation(loadedOperation, true);
        opOperation.makeImmutable();

        exceptionRule.expect(IllegalArgumentException.class);
        blc.addOperation(opOperation);
    }

    @Test
    public void testAddOperationsWhenOperationIsMutable() {
        OpOperation opOperation = new OpOperation();

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Object is mutable");
        blc.addOperation(opOperation);
    }

    @Test
    public void testAddOperationsWhenBlockChainIsLocked() {
        OpOperation opOperation = new OpOperation();
        opOperation.makeImmutable();

        blc.validateLocked(); // LOCKED state

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("This chain is immutable");
        blc.addOperation(opOperation);
    }

    @Test
    public void  testValidateOperations() {
        OpOperation loadedOperation = blc.getQueueOperations().getLast();
        OpOperation opOperation = new OpOperation(loadedOperation, true);
        opOperation.makeImmutable();

        int amountLoadedOperations = blc.getQueueOperations().size();
        blc.removeQueueOperations(new HashSet<>(Collections.singletonList(loadedOperation.getRawHash())));
        assertEquals(amountLoadedOperations - 1, blc.getQueueOperations().size());

        assertTrue(blc.validateOperation(opOperation));
    }

    @Test
    public void  testValidateOperationsIfOperationIsAlreadyExists() {
        OpOperation loadedOperation = blc.getQueueOperations().getLast();
        OpOperation opOperation = new OpOperation(loadedOperation, true);
        opOperation.makeImmutable();

        exceptionRule.expect(IllegalArgumentException.class);
        blc.validateOperation(opOperation);
    }

    @Test
    public void testValidateOperationsWhenOperationIsMutable() {
        OpOperation opOperation = new OpOperation();

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Object is mutable");
        blc.validateOperation(opOperation);
    }

    @Test
    public void testValidateOperationsWhenBlockChainIsLocked() {
        OpOperation opOperation = new OpOperation();
        opOperation.makeImmutable();

        blc.validateLocked(); // LOCKED state

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("This chain is immutable");
        blc.validateOperation(opOperation);
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

    //TODO fix it
    @Ignore
    @Test
    public void testGetBlockHeadersById() throws FailedVerificationException {
	    OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);

	    OpBlock loadedOpBlock = blc.getBlockHeadersById(opBlock.getBlockId());
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
