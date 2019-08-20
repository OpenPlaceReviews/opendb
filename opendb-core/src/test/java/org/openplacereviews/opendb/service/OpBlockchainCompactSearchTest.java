package org.openplacereviews.opendb.service;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.service.DBSchemaManager.OBJS_TABLE;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableWebSecurity
@EnableConfigurationProperties
public class OpBlockchainCompactSearchTest {

	@ClassRule
	public static final PostgreSQLServer databaseServer = new PostgreSQLServer();

	@Rule
	public final PostgreSQLServer.Wiper databaseWiper = new PostgreSQLServer.Wiper();

	@Spy
	@InjectMocks
	private DBConsensusManager dbConsensusManager;

	@Spy
	@InjectMocks
	private DBSchemaManager dbSchemaManager;

	@Spy
	@InjectMocks
	private FileBackupManager fileBackupManager;

	@Spy
	private IPFSFileManager ipfsFileManager;

	@Spy
	private JsonFormatter formatter;

	@Spy
	private LogOperationService logOperationService;

	@Spy
	private HistoryManager historyManager = new HistoryManager();
	private BlocksManager blocksManager = new BlocksManager();
	private OpBlockChain blockChain;
	private OpenDBServer.MetadataDb metadataDb = new OpenDBServer.MetadataDb();
	private JdbcTemplate jdbcTemplate;

	@Before
	public void beforeEachTestMethod() throws Exception {
		MockitoAnnotations.initMocks(this);

		Connection connection = databaseServer.getConnection();
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));

		ReflectionTestUtils.setField(dbConsensusManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(dbConsensusManager, "dbSchema", dbSchemaManager);
		ReflectionTestUtils.setField(dbConsensusManager, "backupManager", fileBackupManager);
		ReflectionTestUtils.setField(historyManager, "dbSchema", dbSchemaManager);
		ReflectionTestUtils.setField(historyManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(historyManager, "blocksManager", blocksManager);
		ReflectionTestUtils.setField(historyManager, "formatter", formatter);
		ReflectionTestUtils.setField(blocksManager, "dataManager", dbConsensusManager);
		ReflectionTestUtils.setField(blocksManager, "serverUser", serverName);
		ReflectionTestUtils.setField(blocksManager, "formatter", formatter);
		ReflectionTestUtils.setField(blocksManager, "historyManager", historyManager);
		ReflectionTestUtils.setField(blocksManager, "extResourceService", ipfsFileManager);
		ReflectionTestUtils.setField(blocksManager, "logSystem", logOperationService);
		Mockito.doNothing().when(fileBackupManager).init();
		Mockito.doNothing().when(ipfsFileManager).processOperations(any());

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());
		Mockito.doCallRealMethod().when(dbConsensusManager).insertOperation(any());

		generateMetadataDB(metadataDb, jdbcTemplate);
		blockChain = dbConsensusManager.init(metadataDb);
		blocksManager.init(metadataDb, blockChain);
		ReflectionTestUtils.setField(blocksManager, "serverKeyPair", serverKeyPair);
		databaseServer.wipeDatabase();
	}

	@After
	public void tearDown() throws Exception {
		databaseServer.wipeDatabase();
	}

	@AfterClass
	public static void afterClassTest() throws SQLException {
		databaseServer.getConnection().close();
	}

	@Test
	public void testSavingNULLObjectsToSuperblock() throws FailedVerificationException {
		generateMore30Blocks(formatter, blockChain, dbConsensusManager, BLOCKCHAIN_LIST);

		dbConsensusManager.saveMainBlockchain(blockChain);

		final long[] amount = new long[1];
		jdbcTemplate.query("SELECT COUNT(*) FROM " + OBJS_TABLE + " WHERE p1 = '22222'", rs -> {
			amount[0] = rs.getLong(1);
		});
		assertEquals(1, amount[0]);

		AtomicReference<OpObject> opObject1 = new AtomicReference<>();
		jdbcTemplate.query("SELECT content FROM " + OBJS_TABLE + " WHERE p1 = '22222'", rs -> {
			opObject1.set(formatter.parseObject(rs.getString(1)));
		});
		assertNull(opObject1.get());

	}

	@Test
	public void testSearchOldObjectVersionInRuntimeBlock() throws FailedVerificationException {
		for (OpOperation op : getOperations(formatter, blocksManager, BLOCKCHAIN_LIST)) {
			assertTrue(blocksManager.addOperation(op));
		}
		blocksManager.createBlock();

		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blockChain.fetchAllObjects("osm.place",  objectsSearchRequest);

		int amountVersions = 0;
		for (OpObject opObject : objectsSearchRequest.result) {
			if (opObject.getId().equals(Collections.singletonList("12345662"))) {
				amountVersions++;
			}
		}
		assertEquals(1, amountVersions);
	}

	@Test
	public void testSearchOldObjectVersionInSuperblock() throws FailedVerificationException {
		List<OpOperation> opOperationList = getOperations(formatter, blocksManager, BLOCKCHAIN_LIST);
		for (int i = 0; i < opOperationList.size(); i++) {
			assertTrue(blocksManager.addOperation(opOperationList.get(i)));
			if (i > 2) {
				blocksManager.createBlock();
			}
		}

		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blockChain.fetchAllObjects("osm.place",  objectsSearchRequest);

		int amountVersions = 0;
		for (OpObject opObject : objectsSearchRequest.result) {
			if (opObject.getId().equals(Collections.singletonList("12345662"))) {
				amountVersions++;
			}
		}
		assertEquals(1, amountVersions);
	}

	@Test
	public void testIndexSearch() throws FailedVerificationException {
		for (OpOperation op : getOperations(formatter, blocksManager, BLOCKCHAIN_LIST)) {
			assertTrue(blocksManager.addOperation(op));
		}
		blocksManager.createBlock();
	}

	@Test
	public void testCompactWithCompactCoefficientEq1RuntimeBlock() throws FailedVerificationException {
		List<OpOperation> opOperationList = getOperations(formatter, blocksManager, BLOCKCHAIN_LIST);
		for (int i = 0; i < opOperationList.size(); i++) {
			assertTrue(blocksManager.addOperation(opOperationList.get(i)));
			if (i > 2) {
				assertNotNull(blocksManager.createBlock());
			}
		}

		assertTrue(blocksManager.compact());
	}

	@Test
	public void testCompactCompactCoefficientEq1AfterBlockCompact() throws FailedVerificationException {
		testCompactWithCompactCoefficientEq1RuntimeBlock();
		assertTrue(blocksManager.compact());
	}

	@Test
	public void testCompactWithCompactCoefficientEq1WithDBBlocks() throws FailedVerificationException {
		ReflectionTestUtils.setField(dbConsensusManager, "superblockSize", 6);
		List<OpOperation> opOperationList = getOperations(formatter, blocksManager, BLOCKCHAIN_LIST);
		for (int i = 0; i < opOperationList.size(); i++) {
			assertTrue(blocksManager.addOperation(opOperationList.get(i)));
			if (i > 2) {
				blocksManager.createBlock();
			}
		}

		assertTrue(blocksManager.compact());
	}

	@Test
	public void testCompactWithCompactCoefficientEq2WithRuntimeBlocks() throws FailedVerificationException {
		ReflectionTestUtils.setField(dbConsensusManager, "compactCoefficient", 2);
		testCompactWithCompactCoefficientEq1RuntimeBlock();
	}

	@Test
	public void testCompactWithCompactCoefficientEq2WithDBBlocks() throws FailedVerificationException {
		ReflectionTestUtils.setField(dbConsensusManager, "compactCoefficient", 2);
		testCompactWithCompactCoefficientEq1WithDBBlocks();
	}
}
