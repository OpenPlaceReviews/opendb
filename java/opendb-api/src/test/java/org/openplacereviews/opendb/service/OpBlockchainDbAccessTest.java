package org.openplacereviews.opendb.service;

import org.junit.*;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainTest;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

public class OpBlockchainDbAccessTest {

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

	@ClassRule
	public static final PostgreSQLServer databaseServer = new PostgreSQLServer();

	private OpBlockchainTest opBlockchainTest;
	private JdbcTemplate jdbcTemplate;
	private OpenDBServer.MetadataDb metadataDb;

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

		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());
		Mockito.doNothing().when(fileBackupManager).init();

		metadataDb = new OpenDBServer.MetadataDb();
		generateMetadataDB(metadataDb, jdbcTemplate);
	}

	@After
	public void afterEachTest() throws Exception {
		databaseServer.wipeDatabase();
	}

	@AfterClass
	public static void afterClassTest() throws SQLException {
		databaseServer.getConnection().close();
	}

	@Ignore
	@Test
	public void testOpBlockchainWithDBAccess() throws FailedVerificationException {
		OpBlock opBlock = opBlockchainTest.blc.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		OpBlockChain blockChain = new OpBlockChain(opBlockchainTest.blc.getParent(), opBlockchainTest.blc.getBlockHeaders(0), dbConsensusManager.createDbAccess(
				opBlockchainTest.blc.getSuperBlockHash(), opBlockchainTest.blc.getSuperblockHeaders()), opBlockchainTest.blc.getRules());

		opBlockchainTest.testOpBlockChain(blockChain, opBlock);
	}


}