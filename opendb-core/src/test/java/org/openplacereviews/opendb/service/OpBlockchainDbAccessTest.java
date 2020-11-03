package org.openplacereviews.opendb.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

	private static final String OBJ_ID = "123456678";
	private static final String OP_ID = "opr.place";
	private static final String OBJ_ID_P1 = "9G2GCG";
	private static final String OBJ_ID_P2 = "wlkomu";
	private static final String OSM_ID = "43383147";

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
	}

	@After
	public void afterEachTest() throws Exception {
		try {
			databaseServer.wipeDatabase();		
		}
		catch (Exception e) {
			e.printStackTrace();
		}
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
		BlocksManager blcManager = createBlocksManager();
		JsonFormatter formatter = new JsonFormatter();
		ReflectionTestUtils.setField(blcManager, "dataManager", dbConsensusManager);
		ReflectionTestUtils.setField(blcManager, "formatter", formatter);
		ReflectionTestUtils.setField(blcManager, "serverKeyPair", serverKeyPair);
		ReflectionTestUtils.setField(blcManager, "serverUser", serverName);
		List<String> bootstrapList =
			    Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
			      "opr-0-test-grant", BlocksManager.BOOT_STD_VALIDATION);
		blcManager.setBootstrapList(bootstrapList);
		blcManager.bootstrap(serverName, serverKeyPair);
        Mockito.doNothing().when(extResourceService).processOperations(ArgumentMatchers.anyList());
		blcManager.createBlock();
		OpOperation placeOp = createPlaceOperation(blockChain);
		blcManager.addOperation(placeOp);
		blcManager.createBlock();
		List<OpOperation> operations = generateStartOpForTest(blockChain);
		for (int i =0 ;i<operations.size();i++) {
			OpOperation opOperation = operations.get(i);
			opOperation.makeImmutable();
			blcManager.addOperation(opOperation);
			OpBlock blockMain = blcManager.createBlock();
		}
		blcManager = createBlocksManager();
		blcManager.init(metadataDb,databaseBlockChain);
		ReflectionTestUtils.setField(blcManager, "dataManager", dbConsensusManager);
		ReflectionTestUtils.setField(blcManager, "formatter", formatter);
		ReflectionTestUtils.setField(blcManager, "serverKeyPair", serverKeyPair);
		ReflectionTestUtils.setField(blcManager, "serverUser", serverName);
		OpOperation editOp = createEditOperation(blockChain);
		blcManager.addOperation(editOp);
		blcManager.createBlock();
		ObjectsSearchRequest req = new ObjectsSearchRequest();
		blockChain.fetchAllObjects(OP_ID, req);
		assertEquals(2, req.result.size());
	}
	
	@Test
	public void testRequestOpFromDb() throws FailedVerificationException {
		String key = OBJ_ID_P1;// + "," + OBJ_ID_P2;
		settingsManager.OPENDB_SUPERBLOCK_SIZE.set(2);
		testAddEditOpToDb();
		BlocksManager blcManager = new BlocksManager();
		blcManager.init(metadataDb,databaseBlockChain);
		OpBlockChain blc = new OpBlockChain(databaseBlockChain.getParent(), databaseBlockChain.getBlockHeaders(0),
								dbConsensusManager.createDbAccess(
										databaseBlockChain.getSuperBlockHash(), databaseBlockChain.getSuperblockHeaders()),
								databaseBlockChain.getRules());
		ObjectsSearchRequest request = new ObjectsSearchRequest();
		blc.fetchAllObjects(OP_ID, request);
		assertEquals(2, request.result.size());
		OpObject obj;
		if (!key.contains(",")) {
			obj = blc.getObjectByName(OP_ID, key);
		} else {
			String[] keys = key.split(",");
			obj = blc.getObjectByName(OP_ID, keys[0].trim(), keys[1].trim());
		}
		Assert.assertNull(obj);
	}

	private BlocksManager createBlocksManager() {
		BlocksManager blcManager = new BlocksManager();
		ReflectionTestUtils.setField(blcManager, "extResourceService", extResourceService);
		ReflectionTestUtils.setField(blcManager, "historyManager", historyManager);
		ReflectionTestUtils.setField(blcManager, "logSystem", logSystem);
		blcManager.init(metadataDb,databaseBlockChain);
		return blcManager;
	}

	
	private OpOperation createEditOperation(OpBlockChain blc) throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType(OP_ID);
		opOperation.setSignedBy(serverName);
		List<Object> edits = new ArrayList<>();
		OpObject edit = new OpObject();
		edit.setId(OBJ_ID_P1, OBJ_ID_P2);
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

	private OpOperation createPlaceOperation(OpBlockChain blc) throws FailedVerificationException {
		String j = "{\"comment\": \"Operation to hold osm places\",\"eval\": {\"parentType\": \"sys.operation\"},\"fields\": {\"id\": \"openplacereview id\",\"osmId\": \"id of osm place\",\"tags\": \"place tags\"},\"id\": [\"opr.place\"],\"version\": 0}";
		return createOpFromJson(blc, j, "sys.operation");
	}
	
	private OpOperation createOpFromJson(OpBlockChain blc, String json, String type) throws FailedVerificationException {
		JsonFormatter jf = new JsonFormatter();
		OpOperation op1 = jf.parseOperation(json);
		OpOperation op1w = new OpOperation();
		op1w.setType(type);
		op1w.setSignedBy(serverName);
		op1w.putObjectValue(OpOperation.F_CREATE, op1);
		op1w.addCreated(op1);
		blc.getRules().generateHashAndSign(op1w, serverKeyPair);
		return op1w;
	}
	
	private List<OpOperation> generateStartOpForTest(OpBlockChain blc) throws FailedVerificationException {
		JsonFormatter jf = new JsonFormatter();

		String version4 = "{\"id\": [\"9G2GCG\",\"wlkomu\"],\"source\": {\"osm\": [{\"id\": 43383147,\"lat\": 50.44494543216025,\"lon\": 30.508777081059165,\"tags\": {\"name\": \"Володимирський собор\",\"amenity\": \"place_of_worship\",\"name:de\": \"Wladimirkathedrale\",\"name:en\": \"St. Vladimir's Cathedral\",\"name:et\": \"Püha Volodõmõri katedraal\",\"name:fr\": \"Cathédrale Saint-Vladimir\",\"name:it\": \"Cattedrale di San Vladimiro\",\"name:nl\": \"Vladimirkathedraal\",\"name:ru\": \"Владимирский cобор\",\"name:uk\": \"Володимирський собор\",\"website\": \"http://www.katedral.org.ua/\",\"building\": \"cathedral\",\"religion\": \"christian\",\"wikidata\": \"Q1417441\",\"wikipedia\": \"uk:Володимирський собор (Київ)\",\"denomination\": \"ukrainian_orthodox\",\"addr:housenumber\": \"20\"},\"type\": \"way\",\"osm_tag\": \"amenity\",\"version\": \"15\",\"changeset\": \"75113080\",\"osm_value\": \"place_of_worship\",\"timestamp\": \"2019-09-30T17:17:23Z\"}],\"old-osm-ids\": []},\"version\": 4,\"placetype\": \"place_of_worship\"}";
		String version1 = "{\"id\": [\"9G2GCG\",\"wlkomu\"],\"images\": {\"outdoor\": [{\"cid\": \"QmTkxczJ6q9XRYz8TTgRiEEZYMMaMvAC2LHMaNkkkKJH96\",\"hash\": \"601141910fd5eff18ac11fb49e402cc4d514c83820001c7fbc84dbc52df93f69\",\"type\": \"#image\",\"extension\": \"jpg\"},{\"cid\": \"QmSDAVFsGUxkxo53kWJynzSDmqD9dz5Kdn46MXgEaHXTDk\",\"hash\": \"be3b18725c8948040507f29d8ec43001145e62ea67a64e08ef82cc1b7b2b0d63\",\"type\": \"#image\",\"extension\": \"jpg\"}]},\"source\": {\"osm\": [{\"id\": 43383147,\"lat\": 50.44494543216025,\"lon\": 30.508777081059165,\"tags\": {\"name\": \"Володимирський собор\",\"amenity\": \"place_of_worship\",\"name:de\": \"Wladimirkathedrale\",\"name:en\": \"St. Vladimir's Cathedral\",\"name:et\": \"Püha Volodõmõri katedraal\",\"name:fr\": \"Cathédrale Saint-Vladimir\",\"name:it\": \"Cattedrale di San Vladimiro\",\"name:nl\": \"Vladimirkathedraal\",\"name:ru\": \"Владимирский cобор\",\"name:uk\": \"Володимирський собор\",\"website\": \"http://www.katedral.org.ua/\",\"building\": \"cathedral\",\"religion\": \"christian\",\"wikidata\": \"Q1417441\",\"wikipedia\": \"uk:Володимирський собор (Київ)\",\"denomination\": \"ukrainian_orthodox\",\"addr:housenumber\": \"20\"},\"type\": \"way\",\"osm_tag\": \"amenity\",\"version\": \"15\",\"changeset\": \"75113080\",\"osm_value\": \"place_of_worship\",\"timestamp\": \"2019-09-30T17:17:23Z\"}],\"old-osm-ids\": []},\"version\": 1,\"placetype\": \"place_of_worship\"}";
		String version15 = "{\"id\": [\"9G2GCG\",\"wlkomu\"],\"source\": {\"osm\": [{\"id\": 43383147,\"lat\": 50.44494543216025,\"lon\": 30.508777081059165,\"tags\": {\"name\": \"Володимирський собор\",\"amenity\": \"place_of_worship\",\"name:de\": \"Wladimirkathedrale\",\"name:en\": \"St. Vladimir's Cathedral\",\"name:et\": \"Püha Volodõmõri katedraal\",\"name:fr\": \"Cathédrale Saint-Vladimir\",\"name:it\": \"Cattedrale di San Vladimiro\",\"name:nl\": \"Vladimirkathedraal\",\"name:ru\": \"Владимирский cобор\",\"name:uk\": \"Володимирський собор\",\"website\": \"http://www.katedral.org.ua/\",\"building\": \"cathedral\",\"religion\": \"christian\",\"wikidata\": \"Q1417441\",\"wikipedia\": \"uk:Володимирський собор (Київ)\",\"denomination\": \"ukrainian_orthodox\",\"addr:housenumber\": \"20\"},\"type\": \"way\",\"osm_tag\": \"amenity\",\"version\": \"15\",\"changeset\": \"75113080\",\"osm_value\": \"place_of_worship\",\"timestamp\": \"2019-09-30T17:17:23Z\"}],\"old-osm-ids\": []},\"placetype\": \"place_of_worship\"}";
		String version7 = "{\"id\": [\"9G2GCG\",\"wlkomu\"],\"images\": [{\"outdoor\": [{\"cid\": \"QmTkxczJ6q9XRYz8TTgRiEEZYMMaMvAC2LHMaNkkkKJH96\",\"hash\": \"601141910fd5eff18ac11fb49e402cc4d514c83820001c7fbc84dbc52df93f69\",\"type\": \"#image\",\"extension\": \"\"}]}],\"source\": {\"osm\": [{\"id\": 43383147,\"lat\": 50.44494543216025,\"lon\": 30.508777081059165,\"tags\": {\"name\": \"Володимирський собор\",\"amenity\": \"place_of_worship\",\"name:de\": \"Wladimirkathedrale\",\"name:en\": \"St. Vladimir's Cathedral\",\"name:et\": \"Püha Volodõmõri katedraal\",\"name:fr\": \"Cathédrale Saint-Vladimir\",\"name:it\": \"Cattedrale di San Vladimiro\",\"name:nl\": \"Vladimirkathedraal\",\"name:ru\": \"Владимирский cобор\",\"name:uk\": \"Володимирський собор\",\"website\": \"http://www.katedral.org.ua/\",\"building\": \"cathedral\",\"religion\": \"christian\",\"wikidata\": \"Q1417441\",\"wikipedia\": \"uk:Володимирський собор (Київ)\",\"denomination\": \"ukrainian_orthodox\",\"addr:housenumber\": \"20\"},\"type\": \"way\",\"osm_tag\": \"amenity\",\"version\": \"15\",\"changeset\": \"75113080\",\"osm_value\": \"place_of_worship\",\"timestamp\": \"2019-09-30T17:17:23Z\"}],\"old-osm-ids\": []},\"version\": 7,\"placetype\": \"place_of_worship\"}";
		String version151 ="{\"id\": [\"9G2GCG\",\"wlkomu\"],\"source\": {\"osm\": [{\"id\": 43383147,\"lat\": 50.44494543216025,\"lon\": 30.508777081059165,\"tags\": {\"name\": \"Володимирський собор\",\"amenity\": \"place_of_worship\",\"name:de\": \"Wladimirkathedrale\",\"name:en\": \"St. Vladimir's Cathedral\",\"name:et\": \"Püha Volodõmõri katedraal\",\"name:fr\": \"Cathédrale Saint-Vladimir\",\"name:it\": \"Cattedrale di San Vladimiro\",\"name:nl\": \"Vladimirkathedraal\",\"name:ru\": \"Владимирский cобор\",\"name:uk\": \"Володимирський собор\",\"website\": \"http://www.katedral.org.ua/\",\"building\": \"cathedral\",\"religion\": \"christian\",\"wikidata\": \"Q1417441\",\"wikipedia\": \"uk:Володимирський собор (Київ)\",\"denomination\": \"ukrainian_orthodox\",\"addr:housenumber\": \"20\"},\"type\": \"way\",\"osm_tag\": \"amenity\",\"version\": \"15\",\"changeset\": \"75113080\",\"osm_value\": \"place_of_worship\",\"timestamp\": \"2019-09-30T17:17:23Z\"}],\"old-osm-ids\": []}}";
		String v14actual = "{\"id\": [\"9G2GCG\",\"wlkomu\"],\"images\": {\"outdoor\": [{\"cid\": \"QmRsY14xMsnwN31Ao6rvTHHsiiPdvaKb17z8W1nwBrpyep\",\"extension\": \"\",\"hash\": \"3b9de9df3d704d50a3f093555637e9992e7618dca798b8256d96de7fa8ac6b8d\",\"type\": \"#image\"}]},\"placetype\": \"place_of_worship\",\"source\": {\"old-osm-ids\": [],\"osm\": [{\"changeset\": \"75113080\",\"id\": 43383147,\"lat\": 50.44494543216025,\"lon\": 30.508777081059165,\"osm_tag\": \"amenity\",\"osm_value\": \"place_of_worship\",\"tags\": {\"addr:housenumber\": \"20\",\"amenity\": \"place_of_worship\",\"building\": \"cathedral\",\"denomination\": \"ukrainian_orthodox\",\"name\": \"Володимирський собор\",\"name:de\": \"Wladimirkathedrale\",\"name:en\": \"St. Vladimir's Cathedral\",\"name:et\": \"Püha Volodõmõri katedraal\",\"name:fr\": \"Cathédrale Saint-Vladimir\",\"name:it\": \"Cattedrale di San Vladimiro\",\"name:nl\": \"Vladimirkathedraal\",\"name:ru\": \"Владимирский cобор\",\"name:uk\": \"Володимирський собор\",\"religion\": \"christian\",\"website\": \"http://www.katedral.org.ua/\",\"wikidata\": \"Q1417441\",\"wikipedia\": \"uk:Володимирський собор (Київ)\"},\"timestamp\": \"2019-09-30T17:17:23Z\",\"type\": \"way\",\"version\": \"15\"}]},\"version\": 14}";
		
		String[] startOperationsJson = { version4, version1, version15, version7, version151 };
		List<OpOperation> startOps = new ArrayList<>();
		for (String s : startOperationsJson) {
			startOps.add(createOpFromJson(blc, s, OP_ID));
		}

		OpOperation initOp = new OpOperation();
		initOp.setType(OP_ID);
		initOp.setSignedBy(serverName);
		OpObject createObj = new OpObject();
		createObj.setId(OBJ_ID_P1, OBJ_ID_P2);
		createObj.setId(OP_ID);
		initOp.putObjectValue(OpOperation.F_CREATE, createObj);
		initOp.addCreated(createObj);
		blc.getRules().generateHashAndSign(initOp, serverKeyPair);
		OpOperation newOpObject = new OpOperation();
		newOpObject.setType(OP_ID);
		newOpObject.setSignedBy(serverName);
		OpObject createObjForNewOpObject = new OpObject();
		createObjForNewOpObject.setId(OBJ_ID_P1, OBJ_ID_P2);
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
		startOps.addAll(0, Arrays.asList(initOp, newOpObject));
		return startOps;
	}
	



}