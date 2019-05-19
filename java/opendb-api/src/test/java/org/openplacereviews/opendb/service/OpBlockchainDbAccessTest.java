package org.openplacereviews.opendb.service;

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainTest;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

public class OpBlockchainDbAccessTest {

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

	@AfterClass
	public static void afterClassTest() throws SQLException {
		databaseServer.getConnection().close();
	}

	@Before
	public void beforeEachTest() throws SQLException, FailedVerificationException {
		MockitoAnnotations.initMocks(this);

		Connection connection = databaseServer.getConnection();
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, false));
		this.opBlockchainTest = new OpBlockchainTest();
		this.opBlockchainTest.beforeEachTestMethod();

		ReflectionTestUtils.setField(dbConsensusManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(dbConsensusManager, "dbSchema", dbSchemaManager);
		ReflectionTestUtils.setField(dbConsensusManager, "backupManager", fileBackupManager);

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());
		Mockito.doNothing().when(fileBackupManager).init();

		metadataDb = new OpenDBServer.MetadataDb();

		generateMetadataDB(metadataDb, jdbcTemplate);

		dbConsensusManager.init(metadataDb);
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

	@Ignore
	@Test
	public void testCreateBlock() throws FailedVerificationException {
		final int amountOperationsForRemoving = 5;
		int i = 0;

		Deque<OpOperation> dequeOperations = opBlockchainTest.blc.getQueueOperations();
		assertFalse(dequeOperations.isEmpty());

		Set<String> operationsToDelete = new HashSet<>();
		List<OpOperation> addOperation = new ArrayList<>();

		Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
		while (i < amountOperationsForRemoving) {
			OpOperation opOperation = iterator.next();
			operationsToDelete.add(opOperation.getRawHash());
			addOperation.add(opOperation);
			i++;
		}

		opBlockchainTest.blc.removeQueueOperations(operationsToDelete);
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		dbConsensusManager.init(metadataDb);
		OpBlockChain opBlockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());
		opBlockChain = dbConsensusManager.saveMainBlockchain(opBlockChain);

		addOperation.forEach(opBlockChain::addOperation);

		//addOperation.forEach(blockChain::addOperation);

		//opBlockchainTest.testCreateBlock(blockChain);
	}

	@Ignore
	@Test
	public void testRemoveQueueOperationsByListOfRowHashes() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());

		exceptionRule.expect(IllegalStateException.class);
		opBlockchainTest.testRemoveQueueOperationsByListOfRowHashes();
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

	@Ignore
	@Test
	public void testAddOperations() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());

		exceptionRule.expect(IllegalStateException.class);
		opBlockchainTest.testAddOperations();
	}

	@Ignore
	@Test
	public void testValidateOperations() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain =
				new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0),
						dbConsensusManager.createDbAccess(
								opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()),
						opBlockchainTest.blc.getRules());

		exceptionRule.expect(IllegalStateException.class);
		opBlockchainTest.testValidateOperations();
	}


}