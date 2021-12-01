package org.openplacereviews.opendb.ops;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.openplacereviews.opendb.ObjectGeneratorTest;
import org.openplacereviews.opendb.service.*;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

public class BlockManagerReplicationTest extends ObjectGeneratorTest {

	public OpBlockChain blc;

	@Spy
	private DBSchemaManager dbSchemaManager;

	@Spy
	public BlocksManager blocksManager;

	@Spy
	public JsonFormatter formatter;

	@Mock
	private IPFSFileManager ipfsFileManager;

	@Mock
	private DBConsensusManager dbConsensusManager;

	@Mock
	private PublicDataManager publicDataManager;

	@Mock
	private LogOperationService logOperationService;

	@Spy
	private SettingsManager settingsManager;

	@Mock
	private HistoryManager historyManager;


	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		MockitoAnnotations.initMocks(this);

		//add patch with fix
		blocksManager.addPatchOperation(1, "place_id_76H3X2_uqbg6o");

		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		blocksManager.init(null, blc);

		ReflectionTestUtils.setField(blocksManager, "publicDataManager", publicDataManager);

		Mockito.doAnswer(invocation -> invocation.getArgument(0)).when(dbConsensusManager).saveMainBlockchain(any());
		Mockito.doCallRealMethod().when(dbConsensusManager).compact(anyInt(), any(), anyBoolean());
		ReflectionTestUtils.setField(dbConsensusManager, "settingsManager", settingsManager);

		Mockito.doAnswer(invocation -> invocation.getArgument(0)).when(dbSchemaManager).setSetting(any(), any(), any());
		ReflectionTestUtils.setField(settingsManager, "dbSchemaManager", dbSchemaManager);

		ReflectionTestUtils.setField(blocksManager, "dataManager", dbConsensusManager);
		ReflectionTestUtils.setField(blocksManager, "serverUser", serverName);
		ReflectionTestUtils.setField(blocksManager, "serverKeyPair", serverKeyPair);
		ReflectionTestUtils.setField(blocksManager, "formatter", formatter);
		ReflectionTestUtils.setField(blocksManager, "historyManager", historyManager);
		ReflectionTestUtils.setField(blocksManager, "extResourceService", ipfsFileManager);
		ReflectionTestUtils.setField(blocksManager, "logSystem", logOperationService);
	}

	@Test
	public void testOperationReplication() throws FailedVerificationException {
		generateOperations(formatter, blc);

		// 1-st block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		boolean replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 2-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr2"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		OpObject opObject = blocksManager.getBlockchain().getObjectByName("osm.place", "76H3X2", "uqbg6o");
		assertEquals("111168845", opObject.getFieldByExpr("source.osm[0].changeset"));

		addOperationFromList(formatter, blocksManager.getBlockchain(), new String[]{"create-obj-fix-opr3"});
		opObject = blocksManager.getBlockchain().getObjectByName("osm.place", "76H3X2", "uqbg6o");
		assertEquals("111168846", opObject.getFieldByExpr("source.osm[0].changeset"));
	}

	@Test
	public void testCompactCoef1() throws FailedVerificationException {
		generateOperations(formatter, blc);

		// 1-st block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		boolean replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 2-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr2"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr3"});

		// 3-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr4"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 4-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr5"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		Mockito.verify(dbConsensusManager, Mockito.times(1)).printBlockChain(any());
	}

	@Test
	public void testCompactCoef2() throws FailedVerificationException {
		settingsManager.OPENDB_COMPACT_COEFICIENT.set(2D);
		generateOperations(formatter, blc);

		// 1-st block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		boolean replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 2-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr2"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr3"});

		// 3-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr4"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 4-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr5"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		Mockito.verify(dbConsensusManager, Mockito.times(2)).printBlockChain(any());
	}

	@Test
	public void testCompactBlocks() throws FailedVerificationException {
		generateOperations(formatter, blc);
		// 1-st block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		blocksManager.replicateOneBlock(opBlock);
		// 2-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr2"});
		opBlock = blc.createBlock(serverName, serverKeyPair);
		blocksManager.replicateOneBlock(opBlock);
		String hash = opBlock.getRawHash().substring(opBlock.getRawHash().length() - 4, opBlock.getRawHash().length() - 1);

		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr3"});
		// 3-nd block
		addOperationFromList(formatter, blc, new String[]{"create-obj-fix-opr4"});
		opBlock = blc.createBlock(serverName, serverKeyPair);

		OpBlockChain blc2 = new OpBlockChain(blc.getParent(), blc.getRules());
		OpBlockChain.DeletedObjectCtx hctx = new OpBlockChain.DeletedObjectCtx();
		blocksManager.patchReplicationBlocks(blc2, opBlock);
		OpBlock res = blc2.replicateBlock(opBlock, hctx);

		ipfsFileManager.processOperations(opBlock);
		dbConsensusManager.insertBlock(res);
		historyManager.saveHistoryForBlockOperations(opBlock, hctx);
		blc.rebaseOperations(blc2);
		blocksManager.compact();

		List<String> blocksChain = new ArrayList<>();
		OpBlockChain p = blc2;
		while (p != null && !p.isNullBlock()) {
			String sh = p.getSuperBlockHash();
			blocksChain.add(sh.substring(sh.length() - 4, sh.length() - 1));
			p = p.getParent();
		}
		assertEquals(2, blocksChain.size());
		assertEquals(blocksChain.get(1), hash);


	}
}
