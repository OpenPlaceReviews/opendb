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
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateUserOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockChain.OP_CHANGE_APPEND;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.*;
import static org.openplacereviews.opendb.ops.OpObject.*;
import static org.openplacereviews.opendb.ops.OpOperation.F_EDIT;
import static org.openplacereviews.opendb.ops.OpOperation.F_REF;

public class BlocksManagerTest {

	private static final String OBJ_ID = "123456678";
	private static final String OP_ID = "osm.testplace";
	private static final String VOTE_OBJ_ID = "vote1234567";

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
	private JsonFormatter formatter;

	private HistoryManager historyManager = new HistoryManager();
	private BlocksManager blocksManager = new BlocksManager();
	private OpBlockChain blockChain;
	private OpenDBServer.MetadataDb metadataDb = new OpenDBServer.MetadataDb();
	private JdbcTemplate jdbcTemplate;

	@Before
	public void beforeEachTestMethod() throws Exception {
		MockitoAnnotations.initMocks(this);

		Connection connection = databaseServer.getConnection();
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, false));

		ReflectionTestUtils.setField(dbConsensusManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(dbConsensusManager, "dbSchema", dbSchemaManager);
		ReflectionTestUtils.setField(dbConsensusManager, "backupManager", fileBackupManager);
		ReflectionTestUtils.setField(historyManager, "dbSchema", dbSchemaManager);
		ReflectionTestUtils.setField(historyManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(historyManager, "blocksManager", blocksManager);
		ReflectionTestUtils.setField(historyManager, "formatter", formatter);
		ReflectionTestUtils.setField(blocksManager, "dataManager", dbConsensusManager);
		ReflectionTestUtils.setField(blocksManager, "serverUser", serverName);
		Mockito.doNothing().when(fileBackupManager).init();

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());

		generateMetadataDB(metadataDb, jdbcTemplate);
		blockChain = dbConsensusManager.init(metadataDb);
		blocksManager.init(metadataDb, blockChain);
		ReflectionTestUtils.setField(blocksManager, "serverKeyPair", serverKeyPair);
		generateUserOperations(formatter, blockChain);
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
	public void testCreatingVoteOperation() throws FailedVerificationException {
		for (OpOperation op : generateStartOperationAndObject()) {
			blocksManager.addOperation(op);
		}

		blocksManager.addOperation(generateVoteObject());
		OpObject voteObj = blockChain.getObjectByName(OP_VOTE, VOTE_OBJ_ID);
		assertNotNull(voteObj);
		assertEquals(F_OPEN, voteObj.getFieldByExpr(F_STATE));
	}

	@Test
	public void testVotingProcessForVoteOperation() throws FailedVerificationException {
		testCreatingVoteOperation();

		for (OpOperation op : generateTwoVoteOperations()) {
			blocksManager.addOperation(op);
		}

		OpObject voteObj = blockChain.getObjectByName(OP_VOTE, VOTE_OBJ_ID);
		assertNotNull(voteObj);

		Map<String, Object> votes = (Map<String, Object>) voteObj.getFieldByExpr(F_VOTES);
		assertEquals(1, ((List) votes.get("positive")).size());
		assertEquals(1, ((List) votes.get("negative")).size());

	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddAlreadyVotedUserToVoteOperation() throws FailedVerificationException {
		testVotingProcessForVoteOperation();

		OpOperation loadOp = generateTwoVoteOperations().get(1);
		loadOp.putObjectValue("hash", "a651b7887142a33cfd57117be59c0d01916e52479fa41cc6a7896bb1491ed6f5");
		blocksManager.addOperation(loadOp);
	}

	@Test
	public void testFinishingVotingProcess() throws FailedVerificationException {
		testVotingProcessForVoteOperation();

		blocksManager.addOperation(generateFinalOpWithRef());

		OpObject editedObj = blockChain.getObjectByName(OP_ID, OBJ_ID);
		assertNotNull(editedObj);
		OpObject voteObj = blockChain.getObjectByName(OP_VOTE, VOTE_OBJ_ID);
		assertNotNull(voteObj);

		assertEquals(F_FINAL, voteObj.getFieldByExpr(F_STATE));
		assertNull(editedObj.getFieldByExpr("lat"));
		assertEquals(12346L, editedObj.getFieldByExpr("lon"));

	}

	@Test(expected = IllegalArgumentException.class)
	public void testFinishingVotingProcessWithNotValidRef() throws FailedVerificationException {
		testVotingProcessForVoteOperation();

		OpOperation finalOp = generateFinalOpWithRef();
		TreeMap<String, Object> refVote = new TreeMap<>();
		refVote.put("v", Arrays.asList(OP_VOTE, ""));
		finalOp.putObjectValue("ref", refVote);

		blocksManager.addOperation(finalOp);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testVoteForFinishedVoteOp() throws FailedVerificationException {
		testFinishingVotingProcess();

		OpOperation notValidVote = generateTwoVoteOperations().get(0);
		notValidVote.putObjectValue("hash", "a651b7887142a33cfd57117be59c0d01916e52479fa41cc6a7896bb1491ed6f5");
		blocksManager.addOperation(notValidVote);

	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddFinishVoteOperationWithNotSameEditOp() throws FailedVerificationException {
		testVotingProcessForVoteOperation();

		OpOperation finalOp = generateFinalOpWithRef();
		finalOp.getEdited().clear();
		finalOp.addEdited(getNotValidEditObj());

		blocksManager.addOperation(finalOp);
	}

	private OpObject getNotValidEditObj() {
		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> change = new TreeMap<>();
		change.put("lon", "delete");
		change.put("lat", "delete");
		TreeMap<String, Object> appendObj = new TreeMap<>();
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", 2222222);
		tagsObject.put("k", 333333333);
		appendObj.put("append", tagsObject);
		change.put("tags", appendObj);
		TreeMap<String, Object> current = new TreeMap<>();
		current.put("lon", 12345);
		current.put("lat", "222EC");
		editObj.putObjectValue(F_CHANGE, change);
		editObj.putObjectValue(F_CURRENT, current);

		return editObj;
	}

	private OpOperation generateFinalOpWithRef() throws FailedVerificationException {
		OpOperation finalOp = new OpOperation();
		finalOp.setType(OP_ID);
		finalOp.setSignedBy(serverName);
		TreeMap<String, Object> refVote = new TreeMap<>();
		refVote.put("vote", Arrays.asList(OP_VOTE, VOTE_OBJ_ID));
		finalOp.putObjectValue(F_REF, refVote);

		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> change = new TreeMap<>();
		change.put("lon", "increment");
		change.put("lat", "delete");
		TreeMap<String, Object> appendObj = new TreeMap<>();
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", 2222222);
		tagsObject.put("k", 333333333);
		appendObj.put("append", tagsObject);
		change.put("tags", appendObj);
		TreeMap<String, Object> current = new TreeMap<>();
		current.put("lon", 12345);
		current.put("lat", "222EC");
		editObj.putObjectValue(F_CHANGE, change);
		editObj.putObjectValue(F_CURRENT, current);

		finalOp.addEdited(editObj);
		blockChain.getRules().generateHashAndSign(finalOp, serverKeyPair);

		return finalOp;
	}

	private List<OpOperation> generateTwoVoteOperations() throws FailedVerificationException {
		OpOperation firstVoteOp = new OpOperation();
		firstVoteOp.setType(OP_VOTE);
		firstVoteOp.setSignedBy("openplacereviews");
		OpObject firstObject = new OpObject();
		firstObject.setId(VOTE_OBJ_ID);
		TreeMap<String, Object> change = new TreeMap<>();
		TreeMap<String, Object> append = new TreeMap<>();
		append.put(OP_CHANGE_APPEND.toString(), Arrays.asList("sys.signup", "openplacereviews"));
		change.put(F_VOTES_POSITIVE, append);
		firstObject.putObjectValue(F_CHANGE, change);
		firstObject.putObjectValue(F_CURRENT, Collections.EMPTY_MAP);
		firstVoteOp.addEdited(firstObject);
		blockChain.getRules().generateHashAndSign(firstVoteOp, serverKeyPair);

		OpOperation secondVoteOp = new OpOperation();
		secondVoteOp.setType(OP_VOTE);
		secondVoteOp.setSignedBy("openplacereviews");
		OpObject secondObj = new OpObject();
		secondObj.setId(VOTE_OBJ_ID);
		TreeMap<String, Object> change2 = new TreeMap<>();
		TreeMap<String, Object> append2 = new TreeMap<>();
		append2.put(OP_CHANGE_APPEND.toString(), Arrays.asList("sys.login", "openplacereviews", "test_1"));
		change2.put(F_VOTES_NEGATIVE, append2);
		secondObj.putObjectValue(F_CHANGE, change2);
		secondObj.putObjectValue(F_CURRENT, Collections.EMPTY_MAP);
		secondVoteOp.addEdited(secondObj);
		blockChain.getRules().generateHashAndSign(secondVoteOp, serverKeyPair);

		return Arrays.asList(firstVoteOp, secondVoteOp);
	}

	private List<OpOperation> generateStartOperationAndObject() throws FailedVerificationException {
		OpOperation initOp = new OpOperation();
		initOp.setType(OP_OPERATION);
		initOp.setSignedBy(serverName);
		OpObject createObj = new OpObject();
		createObj.setId(OP_ID);
		createObj.putObjectValue("version", 1);
		initOp.addCreated(createObj);
		blockChain.getRules().generateHashAndSign(initOp, serverKeyPair);

		OpOperation newOpObject = new OpOperation();
		newOpObject.setType(OP_ID);
		newOpObject.setSignedBy(serverName);
		OpObject createObjForNewOpObject = new OpObject();
		createObjForNewOpObject.setId(OBJ_ID);
		createObjForNewOpObject.putObjectValue("lon", 12345);
		createObjForNewOpObject.putObjectValue("def", 23456);
		createObjForNewOpObject.putObjectValue("lat", "222EC");
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", 11111111);
		tagsObject.put("k", 22222222);
		createObjForNewOpObject.putObjectValue("tags", Collections.singletonList(tagsObject));

		newOpObject.addCreated(createObjForNewOpObject);
		blockChain.getRules().generateHashAndSign(newOpObject, serverKeyPair);

		return Arrays.asList(initOp, newOpObject);
	}

	private OpOperation generateVoteObject() throws FailedVerificationException {
		OpOperation voteOp = new OpOperation();
		voteOp.setType(OP_VOTE);
		voteOp.setSignedBy(serverName);

		OpObject createObj = new OpObject();
		createObj.setId(VOTE_OBJ_ID);
		createObj.putStringValue(F_TYPE, OP_ID);
		createObj.putObjectValue(F_STATE, F_OPEN);

		TreeMap<String, Object> editObj = new TreeMap<>();
		editObj.put(F_ID, OBJ_ID);
		TreeMap<String, Object> change = new TreeMap<>();
		change.put("lon", "increment");
		change.put("lat", "delete");
		TreeMap<String, Object> appendObj = new TreeMap<>();
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", 2222222);
		tagsObject.put("k", 333333333);
		appendObj.put("append", tagsObject);
		change.put("tags", appendObj);
		TreeMap<String, Object> current = new TreeMap<>();
		current.put("lon", 12345);
		current.put("lat", "222EC");
		editObj.put(F_CHANGE, change);
		editObj.put(F_CURRENT, current);

		TreeMap<String, Object> votes = new TreeMap<>();
		votes.put("positive", new ArrayList<>());
		votes.put("negative", new ArrayList<>());
		createObj.putObjectValue(F_VOTES, votes);
		createObj.putObjectValue(F_EDIT, Collections.singletonList(editObj));
		voteOp.addCreated(createObj);


		blockChain.getRules().generateHashAndSign(voteOp, serverKeyPair);

		return voteOp;

	}

}
