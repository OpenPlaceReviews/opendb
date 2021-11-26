package org.openplacereviews.opendb.ops;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.openplacereviews.opendb.ObjectGeneratorTest;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBConsensusManager;
import org.openplacereviews.opendb.service.DBSchemaManager;
import org.openplacereviews.opendb.service.HistoryManager;
import org.openplacereviews.opendb.service.IPFSFileManager;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.SettingsManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.test.util.ReflectionTestUtils;

public class BlockManagerReplicationTest extends ObjectGeneratorTest {

	public OpBlockChain blcMaster;
	
	public OpBlockChain blc;

	public BlocksManager blocksManager;

	public JsonFormatter formatter;
	
	@Mock
	private IPFSFileManager ipfsFileManager;
	
	@Mock
	private DBConsensusManager dbConsensusManager;

	@Mock
	private DBSchemaManager dbSchemaManager;
	
	@Mock
	private PublicDataManager publicDataManager;
	
	@Mock
	private LogOperationService logOperationService;

	@Mock
	private SettingsManager settingsManager;

	@Mock
	private HistoryManager historyManager = new HistoryManager();


	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		MockitoAnnotations.initMocks(this);
		formatter = new JsonFormatter();
		blocksManager = new BlocksManager() {
			public synchronized boolean compact() {
				// TODO
				return false;
			};
		};
		blocksManager.addPatchOperation(1, "place_id_76H3X2_uqbg6o");
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		blocksManager.init(null, new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null)));
		ReflectionTestUtils.setField(blocksManager, "publicDataManager", publicDataManager);
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
		// 1-st block
		generateOperations(formatter, blc);
		addOperationFromList(formatter, blc, new String[] { "create-obj-fix-opr" });
		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		boolean replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);
		
		// 2-nd block
		addOperationFromList(formatter, blc, new String[] { "create-obj-fix-opr2" });
		opBlock = blc.createBlock(serverName, serverKeyPair);
		
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);
		
		OpObject opObject = blocksManager.getBlockchain().getObjectByName("osm.place", "76H3X2", "uqbg6o");
		assertEquals("111168845", opObject.getFieldByExpr("source.osm[0].changeset"));
		
		addOperationFromList(formatter, blocksManager.getBlockchain(), new String[] { "create-obj-fix-opr3" });
		opObject = blocksManager.getBlockchain().getObjectByName("osm.place", "76H3X2", "uqbg6o");
		assertEquals("111168846", opObject.getFieldByExpr("source.osm[0].changeset"));
	}
}
