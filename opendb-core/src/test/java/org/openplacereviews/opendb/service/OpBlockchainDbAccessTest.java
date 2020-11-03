package org.openplacereviews.opendb.service;


import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpBlockchainTest;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

public class OpBlockchainDbAccessTest {

	private static final String OPR_PLACE_TYPE = "opr.place";
	private static final String OBJ_ID_P1 = "9G2GCG";
	private static final String OBJ_ID_P2 = "wlkomu";

	@ClassRule
	public static final PostgreSQLServer databaseServer = new PostgreSQLServer();

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	@Spy
	@InjectMocks
	private DBConsensusManager dbConsensusManager;

	@Spy
	@InjectMocks
	private DBSchemaManager dbSchemaManager;

	@Mock
	private LogOperationService logSystem;

	@Mock
	private HistoryManager historyManager;

	@Spy
	private JsonFormatter formatter;

	@Mock
	private IPFSFileManager extResourceService;

	@Spy
	@InjectMocks
	private FileBackupManager fileBackupManager;
	private OpBlockchainTest opBlockchainTest;
	private JdbcTemplate jdbcTemplate;
	private OpenDBServer.MetadataDb metadataDb;

	@Spy
	private SettingsManager settingsManager;

	@AfterClass
	public static void afterClassTest() throws SQLException {
		databaseServer.getConnection().close();
	}

	public OpBlockChain databaseBlockChain;
	
	private BlocksManager databaseBlocksManager;

	@Before
	public void beforeEachTest() throws SQLException, FailedVerificationException {
		MockitoAnnotations.initMocks(this);

		Connection connection = databaseServer.getConnection();
		SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource(connection, true);
		this.jdbcTemplate = new JdbcTemplate(singleConnectionDataSource);
		this.opBlockchainTest = new OpBlockchainTest();
		this.opBlockchainTest.beforeEachTestMethod();

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

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());
		Mockito.doNothing().when(fileBackupManager).init();

		metadataDb = new OpenDBServer.MetadataDb();

		generateMetadataDB(metadataDb, jdbcTemplate);

		databaseBlockChain = dbConsensusManager.init(metadataDb);
		
		databaseBlocksManager = new BlocksManager();
		ReflectionTestUtils.setField(databaseBlocksManager, "extResourceService", extResourceService);
		ReflectionTestUtils.setField(databaseBlocksManager, "historyManager", historyManager);
		ReflectionTestUtils.setField(databaseBlocksManager, "logSystem", logSystem);
		ReflectionTestUtils.setField(databaseBlocksManager, "dataManager", dbConsensusManager);
		ReflectionTestUtils.setField(databaseBlocksManager, "formatter", formatter);
		ReflectionTestUtils.setField(databaseBlocksManager, "serverKeyPair", serverKeyPair);
		ReflectionTestUtils.setField(databaseBlocksManager, "serverUser", serverName);
		databaseBlocksManager.init(metadataDb, databaseBlockChain);
	}

	@After
	public void afterEachTest() throws Exception {
		try {
			databaseServer.wipeDatabase();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testOpBlockchainWithDBAccess() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain = new OpBlockChain(opBlockchainTest.blc.getParent(),
				opBlockchainTest.blc.getBlockHeaders(0),
				dbConsensusManager.createDbAccess(opBlockchainTest.blc.getSuperBlockHash(),
						opBlockchainTest.blc.getSuperblockHeaders()),
				opBlockchainTest.blc.getRules());

		exceptionRule.expect(UnsupportedOperationException.class);
		opBlockchainTest.testOpBlockChain(blockChain);
	}

	@Test
	public void testOpBlockChainWithNotEqualParentsWithDBAccess() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain = new OpBlockChain(opBlockchainTest.blc.getParent(),
				opBlockchainTest.blc.getBlockHeaders(0),
				dbConsensusManager.createDbAccess(opBlockchainTest.blc.getSuperBlockHash(),
						opBlockchainTest.blc.getSuperblockHeaders()),
				opBlockchainTest.blc.getRules());

		exceptionRule.expect(IllegalStateException.class);
		opBlockchainTest.testOpBlockChainWithNotEqualParents(blockChain);
	}

	@Test
	public void testRebaseOperations() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain = new OpBlockChain(opBlockchainTest.blc.getParent(),
				opBlockchainTest.blc.getBlockHeaders(0),
				dbConsensusManager.createDbAccess(opBlockchainTest.blc.getSuperBlockHash(),
						opBlockchainTest.blc.getSuperblockHeaders()),
				opBlockchainTest.blc.getRules());

		exceptionRule.expect(UnsupportedOperationException.class);
		opBlockchainTest.testRebaseOperations(blockChain);
	}

	@Test
	public void testChangeToEqualParent() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain = new OpBlockChain(opBlockchainTest.blc.getParent(),
				opBlockchainTest.blc.getBlockHeaders(0),
				dbConsensusManager.createDbAccess(opBlockchainTest.blc.getSuperBlockHash(),
						opBlockchainTest.blc.getSuperblockHeaders()),
				opBlockchainTest.blc.getRules());

		exceptionRule.expect(UnsupportedOperationException.class);
		opBlockchainTest.testChangeToEqualParent(blockChain);
	}

	
	@Test
	public void testRequestPartialKeyFromDbIsNull() throws FailedVerificationException {
		// bug was realted to small blocks stored in database
		settingsManager.OPENDB_SUPERBLOCK_SIZE.set(2);
		populateDBWithAddAndEdit();
		OpObject obj = databaseBlockChain.getObjectByName(OPR_PLACE_TYPE, OBJ_ID_P1);
		Assert.assertNull(obj);
	}

	@Test
	public void testRequestOpFromDbNotNull() throws FailedVerificationException {
		settingsManager.OPENDB_SUPERBLOCK_SIZE.set(2);
		populateDBWithAddAndEdit();
		OpObject obj = databaseBlockChain.getObjectByName(OPR_PLACE_TYPE, OBJ_ID_P1, OBJ_ID_P2);
		Assert.assertNotNull(obj);
	}


	private OpOperation createEditOperation(String key, String key2, int ver) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OPR_PLACE_TYPE);
		OpObject edit = new OpObject();
		edit.setId(OBJ_ID_P1, OBJ_ID_P2);
		
		List<Object> imageResponseList = new ArrayList<>();
		Map<String, Object> imageMap = new TreeMap<>();
		imageMap.put("cid", "__OPRImage.cid_" + ver);
		imageMap.put("hash", "__OPRImage.hash_" + ver);
		imageMap.put("extension", "__OPRImage.extension_" + ver);
		imageMap.put("type", "__OPRImage.type_" + ver);
		imageResponseList.add(imageMap);
		
		Map<String, Object> images = new TreeMap<>();
		Map<String, Object> outdoor = new TreeMap<>();
		outdoor.put("outdoor", imageResponseList);
		images.put("append", outdoor);
		
		
		Map<String, Object> change = new TreeMap<>();
		change.put("version", "increment");
		change.put("images", images);
		edit.putObjectValue(OpObject.F_CHANGE, change);
		
		// with append current is empty
		edit.putObjectValue(OpObject.F_CURRENT, new TreeMap<>());
		
		opOperation.addEdited(edit);
		return opOperation;
	}

	private OpOperation createPlaceOperation() throws FailedVerificationException {
		String obj = "{\"comment\": \"Operation to hold osm places\",\"eval\": {\"parentType\": \"sys.operation\"},\"fields\": {\"id\": \"openplacereview id\",\"osmId\": \"id of osm place\",\"tags\": \"place tags\"},\"id\": [\"opr.place\"],\"version\": 0}";
		OpOperation op = new OpOperation();
		op.setType("sys.operation");
		op.addCreated(formatter.parseObject(obj));
		return op;
	}
	
	private OpOperation generateCreatePlaceOp(String key, String key2) throws FailedVerificationException {
		OpOperation op = new OpOperation();
		op.setType(OPR_PLACE_TYPE);
		op.setSignedBy(serverName);
		OpObject obj = new OpObject();
		obj.setId(key, key2);
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", "value");
		tagsObject.put("k", "key");
		obj.putObjectValue("tags", tagsObject);
		// firstObj.setId(OP_ID);
		op.putObjectValue(OpOperation.F_CREATE, obj);
		op.addCreated(obj);
		return op;
	}
	
	private void populateDBWithAddAndEdit() throws FailedVerificationException {
		
		List<String> bootstrapList = Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS,
				BlocksManager.BOOT_STD_ROLES, "opr-0-test-grant", BlocksManager.BOOT_STD_VALIDATION);
		databaseBlocksManager.setBootstrapList(bootstrapList);
		databaseBlocksManager.bootstrap(serverName, serverKeyPair);
		Mockito.doNothing().when(extResourceService).processOperations(ArgumentMatchers.anyList());
		databaseBlocksManager.createBlock();
		
		addOpAndBlock(createPlaceOperation());
		
		addOpAndBlock(generateCreatePlaceOp(OBJ_ID_P1, OBJ_ID_P2));

		addOpAndBlock(generateCreatePlaceOp(OBJ_ID_P1, OBJ_ID_P2 +"2"));
		// create 4 edit versions 
		for(int i = 1; i < 5; i++) {
			OpOperation editOp = createEditOperation(OBJ_ID_P1, OBJ_ID_P2, i);
			addOpAndBlock(editOp);
		}
		
		ObjectsSearchRequest req = new ObjectsSearchRequest();
		databaseBlockChain.fetchAllObjects(OPR_PLACE_TYPE, req);
		assertEquals(2, req.result.size());
		OpObject first = req.result.get(0);
		assertEquals(4, first.getListStringMap("images").size());
		assertTrue(4 == ((Number) first.getObjectValue("version")).intValue());
		
		OpObject second = req.result.get(1);
		assertNull(second.getListStringMap("images"));
		assertNull(second.getObjectValue("version"));
	}

	private void addOpAndBlock(OpOperation op) throws FailedVerificationException {
		op.setSignedBy(databaseBlocksManager.getServerUser());
		databaseBlocksManager.generateHashAndSign(op, databaseBlocksManager.getServerLoginKeyPair());
		databaseBlocksManager.addOperation(op);
		databaseBlocksManager.createBlock();
	}

}