package org.openplacereviews.opendb.service;

import org.junit.*;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

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

	private static final String opType = "osm.place";
	private static final String table = "obj_osm";

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
		ReflectionTestUtils.setField(dbSchemaManager, "objtables", generateObjtables());
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
		jdbcTemplate.query("SELECT COUNT(*) FROM " + table + " WHERE p1 = '22222'", rs -> {
			amount[0] = rs.getLong(1);
		});
		assertEquals(1, amount[0]);

		AtomicReference<OpObject> opObject1 = new AtomicReference<>();
		jdbcTemplate.query("SELECT content FROM " + table + " WHERE p1 = '22222'", rs -> {
			opObject1.set(formatter.parseObject(rs.getString(1)));
		});
		assertNull(opObject1.get());
	}

	@Test
	public void testSearchWithoutDuplicatesAfterEditInRuntimeBlocks() throws FailedVerificationException {
		for (OpOperation op : getOperations(formatter, blocksManager, BLOCKCHAIN_LIST)) {
			assertTrue(blocksManager.addOperation(op));
		}
		blocksManager.createBlock();

		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blockChain.fetchAllObjects(opType,  objectsSearchRequest);

		int duplicates = 0;
		boolean deletedExist = false;
		for (OpObject opObject : objectsSearchRequest.result) {
			if (opObject.getId().equals(Collections.singletonList("12345662"))) {
				duplicates++;
			} else if (opObject.getId().equals(Collections.singletonList("22222"))) {
				deletedExist = true;
			}
		}
		assertEquals(1, duplicates);
		assertFalse(deletedExist);
	}

	@Test
	public void testSearchWithoutDuplicatesAfterEditInSuperblock() throws FailedVerificationException {
		List<OpOperation> opOperationList = getOperations(formatter, blocksManager, BLOCKCHAIN_LIST);
		for (int i = 0; i < opOperationList.size(); i++) {
			assertTrue(blocksManager.addOperation(opOperationList.get(i)));
			if (i > 2) {
				blocksManager.createBlock();
			}
		}

		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blockChain.fetchAllObjects(opType,  objectsSearchRequest);

		int duplicates = 0;
		for (OpObject opObject : objectsSearchRequest.result) {
			if (opObject.getId().equals(Collections.singletonList("12345662"))) {
				duplicates++;
			}
		}
		assertEquals(1, duplicates);
	}

	@Test
	public void testSearchWithoutDuplicatesAfterEditInCompactDB() throws FailedVerificationException {
		ReflectionTestUtils.setField(dbConsensusManager, "compactCoefficient", 2);
		testSearchWithoutDuplicatesAfterEditInSuperblock();
	}

	@Test
	public void testIndexSearchWithRuntimeBlocks() throws FailedVerificationException {
		for (OpOperation op : getOperations(formatter, blocksManager, BLOCKCHAIN_LIST)) {
			assertTrue(blocksManager.addOperation(op));
		}
		blocksManager.createBlock();

		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blocksManager.getObjectsByIndex(opType, "osmid", objectsSearchRequest, 232423451);

		assertEquals(1, objectsSearchRequest.result.size());
	}

	@Test
	public void testIndexSearchWithDBBlocks() throws FailedVerificationException {
		ReflectionTestUtils.setField(dbConsensusManager, "superblockSize", 6);
		List<OpOperation> opOperationList = getOperations(formatter, blocksManager, BLOCKCHAIN_LIST);
		for (int i = 0; i < opOperationList.size(); i++) {
			assertTrue(blocksManager.addOperation(opOperationList.get(i)));
			if (i > 2) {
				assertNotNull(blocksManager.createBlock());
			}
		}

		OpBlockChain.ObjectsSearchRequest objectsSearchRequest = new OpBlockChain.ObjectsSearchRequest();
		blocksManager.getObjectsByIndex(opType, "osmid", objectsSearchRequest, 232423451);

		assertEquals(1, objectsSearchRequest.result.size());
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

	private TreeMap<String, Map<String, Object>> generateObjtables() {
		TreeMap<String, Map<String, Object>> objtables = new TreeMap<String, Map<String, Object>>();
		Map<String, Object> objectMap = new TreeMap<>();
		TreeMap<String, Object> linkedTypesMap = new TreeMap<>();
		linkedTypesMap.put("0", opType);
		objectMap.put("types", linkedTypesMap);
		objectMap.put("keysize", 2);

		TreeMap<String, Object> columnObject = new TreeMap<>();
		columnObject.put("name", "osmid");
		TreeMap<String, Object> fieldLinkedMap = new TreeMap<>();
		fieldLinkedMap.put("0", "osm.id");
		columnObject.put("field", fieldLinkedMap);
		columnObject.put("sqlmapping", "array");
		columnObject.put("sqltype", "bigint[]");
		columnObject.put("index", "GIN");
		TreeMap<String, Object> linkedColumnMap = new TreeMap<>();
		linkedColumnMap.put("0", columnObject);

		objectMap.put("columns", linkedColumnMap);
		objtables.put(table, objectMap);

		return objtables;
	}
}
