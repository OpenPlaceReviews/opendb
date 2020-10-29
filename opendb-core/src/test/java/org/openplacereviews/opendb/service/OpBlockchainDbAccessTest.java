package org.openplacereviews.opendb.service;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.ObjectGeneratorTest;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_OPERATION;

public class OpBlockchainDbAccessTest {

	private static final String OBJ_ID = "123456678";
	private static final String OP_ID = "opr.place";

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

	@Spy
	private JsonFormatter formatter;

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

	@Before
	public void beforeEachTest() throws SQLException, FailedVerificationException {
		MockitoAnnotations.initMocks(this);

		Connection connection = databaseServer.getConnection();
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, false));
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
	}

	@After
	public void afterEachTest() throws Exception {
		databaseServer.wipeDatabase();
	}

	@Test
	public void testOpBlockchainWithDBAccess() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());

		exceptionRule.expect(UnsupportedOperationException.class);
		opBlockchainTest.testOpBlockChain(blockChain);
	}

	@Test
	public void testOpBlockChainWithNotEqualParentsWithDBAccess() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());

		exceptionRule.expect(IllegalStateException.class);
		opBlockchainTest.testOpBlockChainWithNotEqualParents(blockChain);
	}

	@Test
	public void testRebaseOperations() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());

		exceptionRule.expect(UnsupportedOperationException.class);
		opBlockchainTest.testRebaseOperations(blockChain);
	}

	@Test
	public void testChangeToEqualParent() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());

		exceptionRule.expect(UnsupportedOperationException.class);
		opBlockchainTest.testChangeToEqualParent(blockChain);
	}

	@Test
	public void testAddEditOpToDb() throws FailedVerificationException {
		OpBlockChain blockChain = databaseBlockChain;
		JsonFormatter formatter = new JsonFormatter();
		ObjectGeneratorTest.generateUserOperations(formatter, blockChain);
		List<OpOperation> operations = generateStartOpForTest(blockChain);
		for (OpOperation opOperation : operations) {
			opOperation.makeImmutable();
			blockChain.addOperation(opOperation);
		}
		ObjectsSearchRequest req = new ObjectsSearchRequest();
		blockChain.fetchAllObjects(OP_ID, req);
		assertEquals(1, req.result.size());
		OpOperation editOp = createEditOperation(blockChain);
		assertTrue(blockChain.addOperation(editOp));
	}

	private OpOperation createEditOperation(OpBlockChain blc) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OP_ID);
		opOperation.setSignedBy(serverName);
		List<Object> edits = new ArrayList<>();
		OpObject edit = new OpObject();
		edit.setId(OBJ_ID);
		List<Object> imageResponseList = new ArrayList<>();
		Map<String, Object> imageMap = new TreeMap<>();
		imageMap.put("cid", "__OPRImage.cid");
		imageMap.put("hash", "__OPRImage.hash");
		imageMap.put("extension", "__OPRImage.extension");
		imageMap.put("type", "__OPRImage.type");
		imageResponseList.add(imageMap);
		List<String> ids = new ArrayList<>(Arrays.asList("__placeId"));
		Map<String, Object> change = new TreeMap<>();
		Map<String, Object> images = new TreeMap<>();
		Map<String, Object> outdoor = new TreeMap<>();
		outdoor.put("outdoor", imageResponseList);
		images.put("append", outdoor);
		change.put("version", "increment");
		change.put("images", images);
		TreeMap<String, Object> setAlreadyExist = new TreeMap<>();
		setAlreadyExist.put("images", new ArrayList<>());
		TreeMap<String, Object> current = new TreeMap<>();
		edit.putObjectValue(OpObject.F_CHANGE, change);
		edit.putObjectValue(OpObject.F_CURRENT, current);
		edits.add(edit);
		opOperation.putObjectValue(OpOperation.F_EDIT, edit);
		opOperation.addEdited(edit);
		blc.getRules().generateHashAndSign(opOperation, serverKeyPair);
		opOperation.makeImmutable();
		return opOperation;
	}

	private List<OpOperation> generateStartOpForTest(OpBlockChain blc) throws FailedVerificationException {
		OpOperation initOp = new OpOperation();
		initOp.setType(OP_OPERATION);
		initOp.setSignedBy(serverName);
		OpObject createObj = new OpObject();
		createObj.setId(OP_ID);
		initOp.putObjectValue(OpOperation.F_CREATE, createObj);
		initOp.addCreated(createObj);
		blc.getRules().generateHashAndSign(initOp, serverKeyPair);
		OpOperation newOpObject = new OpOperation();
		newOpObject.setType(OP_ID);
		newOpObject.setSignedBy(serverName);
		OpObject createObjForNewOpObject = new OpObject();
		createObjForNewOpObject.setId(OBJ_ID);
		TreeMap<String, Object> lonObject = new TreeMap<>();
		lonObject.put("k", 123456);
		createObjForNewOpObject.putObjectValue("lon", lonObject);
		createObjForNewOpObject.putObjectValue("def", 23456);
		createObjForNewOpObject.putObjectValue("lat", "222EC");
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", Arrays.asList("23423423423"));
		tagsObject.put("k", Collections.emptyMap());
		createObjForNewOpObject.putObjectValue("tags", tagsObject);
		newOpObject.putObjectValue(OpOperation.F_CREATE, createObjForNewOpObject);
		newOpObject.addCreated(createObjForNewOpObject);
		blc.getRules().generateHashAndSign(newOpObject, serverKeyPair);
		return Arrays.asList(initOp, newOpObject);
	}
}