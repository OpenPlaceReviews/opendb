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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateUserOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockChain.OP_CHANGE_APPEND;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_OPERATION;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_VOTE;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_VOTING;
import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;

public class BlocksManagerTest {

	private static final String OBJ_ID = "osm.place";

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
		for (OpOperation op : generateInitAndEditOp()) {
			blocksManager.addOperation(op);
		}

		OpOperation editOp = generateInitAndEditOp().get(1);
		OpObject voteObject = blockChain.getObjectByName(OP_VOTE, Arrays.asList(editOp.getRawHash(), OBJ_ID));
		assertNotNull(voteObject);

		// TODO add validation on OP status
	}

	@Test
	public void testVotingProcessForVoteOperation() throws FailedVerificationException {
		testCreatingVoteOperation();

		OpOperation editOp = generateInitAndEditOp().get(1);

		for (OpOperation op : generateTwoAppendOperations(editOp.getRawHash())) {
			blocksManager.addOperation(op);
		}

		OpObject voteObject = blockChain.getObjectByName(OP_VOTE, Arrays.asList(editOp.getRawHash(), OBJ_ID));
		assertEquals(((List<Object>)voteObject.getFieldByExpr("votes")).size(), 2);
	}

	// TODO add op with not valid vote

	@Test
	public void testFinishingVotingProcess() throws FailedVerificationException {
		testVotingProcessForVoteOperation();

		OpOperation editOp = generateInitAndEditOp().get(1);
		blocksManager.addOperation(generateFinishVotingOp(editOp.getRawHash()));

		OpObject editedObj = blockChain.getObjectByName(OP_OPERATION, OBJ_ID);
		assertEquals(2L, editedObj.getFieldByExpr("version"));
	}

	// TODO add op with not valid ref

	private OpOperation generateFinishVotingOp(String opHash) throws FailedVerificationException {
		OpOperation finishVoting = new OpOperation();
		finishVoting.setType(OP_VOTING);
		finishVoting.setSignedBy(serverName);
		TreeMap<String, Object> refVote = new TreeMap<>();
		refVote.put("v", Arrays.asList(OP_VOTE, opHash, OBJ_ID));
		finishVoting.putObjectValue("ref", refVote);
		blockChain.getRules().generateHashAndSign(finishVoting, serverKeyPair);

		return finishVoting;
	}

	private List<OpOperation> generateTwoAppendOperations(String opHash) throws FailedVerificationException {
		OpOperation firstVoteOp = new OpOperation();
		firstVoteOp.setType(OP_VOTE);
		firstVoteOp.setSignedBy("openplacereviews");
		OpObject firstObject = new OpObject();
		firstObject.setId(opHash, OBJ_ID);
		TreeMap<String, Object> change = new TreeMap<>();
		TreeMap<String, Object> append = new TreeMap<>();
		append.put(OP_CHANGE_APPEND.toString(), Arrays.asList("sys.signup", "openplacereviews"));
		change.put("votes", append);
		firstObject.putObjectValue(F_CHANGE, change);
		firstObject.putObjectValue(F_CURRENT, Collections.EMPTY_MAP);
		firstVoteOp.addEdited(firstObject);
		blockChain.getRules().generateHashAndSign(firstVoteOp, serverKeyPair);

		OpOperation secondVoteOp = new OpOperation();
		secondVoteOp.setType(OP_VOTE);
		secondVoteOp.setSignedBy("openplacereviews");
		OpObject secondObj = new OpObject();
		secondObj.setId(opHash, OBJ_ID);
		TreeMap<String, Object> change2 = new TreeMap<>();
		TreeMap<String, Object> append2 = new TreeMap<>();
		append2.put(OP_CHANGE_APPEND.toString(), Arrays.asList("sys.login", "openplacereviews","test_1"));
		change2.put("votes", append2);
		secondObj.putObjectValue(F_CHANGE, change2);
		secondObj.putObjectValue(F_CURRENT, Collections.EMPTY_MAP);
		secondVoteOp.addEdited(secondObj);
		blockChain.getRules().generateHashAndSign(secondVoteOp, serverKeyPair);

		return Arrays.asList(firstVoteOp, secondVoteOp);
	}

	private List<OpOperation> generateInitAndEditOp() throws FailedVerificationException {
		OpOperation initOp = new OpOperation();
		initOp.setType(OP_OPERATION);
		initOp.setSignedBy(serverName);
		OpObject createObj = new OpObject();
		createObj.setId(OBJ_ID);
		createObj.putObjectValue("version", 1);
		initOp.addCreated(createObj);
		blockChain.getRules().generateHashAndSign(initOp, serverKeyPair);

		OpOperation editOp = new OpOperation();
		editOp.setType(OP_OPERATION);
		editOp.setSignedBy("openplacereviews");
		OpObject editObj = new OpObject();
		editObj.setId("osm.place");
		TreeMap<String, Object> changeObject = new TreeMap<>();
		changeObject.put("version", "increment");
		editObj.putObjectValue(F_CHANGE, changeObject);
		editObj.putObjectValue(F_CURRENT, Collections.EMPTY_MAP);
		editOp.addEdited(editObj);

		blockChain.getRules().generateHashAndSign(editOp, serverKeyPair);

		return Arrays.asList(initOp, editOp);
	}



}
