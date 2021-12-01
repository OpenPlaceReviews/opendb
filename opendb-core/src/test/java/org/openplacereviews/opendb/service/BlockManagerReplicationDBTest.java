package org.openplacereviews.opendb.service;

import org.junit.*;
import org.mockito.*;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import static org.junit.Assert.*;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

public class BlockManagerReplicationDBTest {

	@ClassRule
	public static final PostgreSQLServer databaseServer = new PostgreSQLServer();

	@Rule
	public final PostgreSQLServer.Wiper databaseWiper = new PostgreSQLServer.Wiper();

	public OpBlockChain opBlockChain;

	@Spy
	@InjectMocks
	private DBConsensusManager dbConsensusManager;

	@Spy
	@InjectMocks
	private DBSchemaManager dbSchemaManager;

	@Spy
	public BlocksManager blocksManager;

	@Spy
	private JsonFormatter formatter;

	@Spy
	private SettingsManager settingsManager;

	@Spy
	@InjectMocks
	private FileBackupManager fileBackupManager;
	private JdbcTemplate jdbcTemplate;
	private OpenDBServer.MetadataDb metadataDb;

	@Mock
	private PublicDataManager publicDataManager;

	@Mock
	private LogOperationService logOperationService;

	@Mock
	private IPFSFileManager ipfsFileManager;

	@Mock
	private HistoryManager historyManager;


	@Before
	public void beforeEachTestMethod() throws Exception {
		MockitoAnnotations.initMocks(this);

		//add patch with fix
		blocksManager.addPatchOperation(1, "place_id_76H3X2_uqbg6o");

		Connection connection = databaseServer.getConnection();
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));

		DataSourceTransactionManager txManager = new DataSourceTransactionManager();
		txManager.setDataSource(jdbcTemplate.getDataSource());
		TransactionTemplate txTemplate = new TransactionTemplate();
		txTemplate.setTransactionManager(txManager);
		ReflectionTestUtils.setField(dbConsensusManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(dbConsensusManager, "dbSchema", dbSchemaManager);
		ReflectionTestUtils.setField(dbConsensusManager, "backupManager", fileBackupManager);
		ReflectionTestUtils.setField(dbConsensusManager, "txTemplate", txTemplate);
		ReflectionTestUtils.setField(settingsManager, "dbSchemaManager", dbSchemaManager);
		ReflectionTestUtils.setField(settingsManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(dbConsensusManager, "settingsManager", settingsManager);

		ReflectionTestUtils.setField(blocksManager, "publicDataManager", publicDataManager);
		ReflectionTestUtils.setField(blocksManager, "dataManager", dbConsensusManager);
		ReflectionTestUtils.setField(blocksManager, "serverUser", serverName);
		ReflectionTestUtils.setField(blocksManager, "serverKeyPair", serverKeyPair);
		ReflectionTestUtils.setField(blocksManager, "formatter", formatter);
		ReflectionTestUtils.setField(blocksManager, "historyManager", historyManager);
		ReflectionTestUtils.setField(blocksManager, "extResourceService", ipfsFileManager);
		ReflectionTestUtils.setField(blocksManager, "logSystem", logOperationService);

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());
		Mockito.doCallRealMethod().when(dbSchemaManager).insertObjIntoTableBatch(any(), any(), any(), any());

		Mockito.doNothing().when(fileBackupManager).init();

		metadataDb = new OpenDBServer.MetadataDb();
		generateMetadataDB(metadataDb, jdbcTemplate);

		opBlockChain = dbConsensusManager.init(metadataDb);
		blocksManager.init(metadataDb, opBlockChain);
	}

	@After
	public void tearDown() throws Exception {
		databaseServer.wipeDatabase();
	}

	@Test
	public void testOperationReplicationDB() throws FailedVerificationException {
		generateOperations(formatter, opBlockChain);

		// 1-st block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		boolean replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 2-nd block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr2"});
		opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		OpObject opObject = blocksManager.getBlockchain().getObjectByName("osm.place", "76H3X2", "uqbg6o");
		assertEquals("111168845", opObject.getFieldByExpr("source.osm[0].changeset"));

		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr3"});
		opObject = opBlockChain.getObjectByName("osm.place", "76H3X2", "uqbg6o");
		assertEquals("111168846", opObject.getFieldByExpr("source.osm[0].changeset"));
	}

	@Test
	public void testCompactCallOneTime() throws FailedVerificationException {
		generateOperations(formatter, opBlockChain);

		// 1-st block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		boolean replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 2-nd block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr2"});
		opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr3"});

		// 3-nd block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr4"});
		opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		Mockito.verify(dbConsensusManager, Mockito.times(1)).printBlockChain(any());

	}

	@Test
	public void testCompactSuperBlockSize1() throws FailedVerificationException {
		settingsManager.OPENDB_SUPERBLOCK_SIZE.set(1);

		generateOperations(formatter, opBlockChain);

		// 1-st block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		boolean replicate = blocksManager.replicateOneBlock(opBlock);
		assertTrue("Replication failed", replicate);

		// 2-nd block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr2"});
		opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		Mockito.doThrow(new IllegalArgumentException()).when(blocksManager).replicateOneBlock(opBlock);
		Exception exception = null;

		try {
			blocksManager.replicateOneBlock(opBlock);
		} catch (IllegalArgumentException t) {
			exception = t;
		}

		assertNotNull(exception);
	}

	@Test
	public void testCompactBlocks() throws FailedVerificationException {
		generateOperations(formatter, opBlockChain);

		// 1-st block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr"});
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		blocksManager.replicateOneBlock(opBlock);

		// 2-nd block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr2"});
		opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		blocksManager.replicateOneBlock(opBlock);
		String hash = opBlock.getRawHash().substring(opBlock.getRawHash().length() - 4, opBlock.getRawHash().length() - 1);

		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr3"});

		// 3-nd block
		addOperationFromList(formatter, opBlockChain, new String[]{"create-obj-fix-opr4"});
		opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		OpBlockChain blc2 = new OpBlockChain(opBlockChain.getParent(), opBlockChain.getRules());
		OpBlockChain.DeletedObjectCtx hctx = new OpBlockChain.DeletedObjectCtx();
		blocksManager.patchReplicationBlocks(blc2, opBlock);
		OpBlock res = blc2.replicateBlock(opBlock, hctx);

		ipfsFileManager.processOperations(opBlock);
		dbConsensusManager.insertBlock(res);
		historyManager.saveHistoryForBlockOperations(opBlock, hctx);
		opBlockChain.rebaseOperations(blc2);
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
