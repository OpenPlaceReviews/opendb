package org.openplacereviews.opendb.service;

import org.junit.*;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Connection;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.*;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.service.DBSchemaManager.*;

public class DBConsensusManagerTest {

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
	private JsonFormatter formatter;

	@Spy
	@InjectMocks
	private FileBackupManager fileBackupManager;
	private JdbcTemplate jdbcTemplate;
	private OpenDBServer.MetadataDb metadataDb;

	@Before
	public void beforeEachTestMethod() throws Exception {
		MockitoAnnotations.initMocks(this);

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

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());
		Mockito.doCallRealMethod().when(dbSchemaManager).insertObjIntoTableBatch(any(), any(), any(), any());

		Mockito.doNothing().when(fileBackupManager).init();

		metadataDb = new OpenDBServer.MetadataDb();
		generateMetadataDB(metadataDb, jdbcTemplate);
	}

	@After
	public void tearDown() throws Exception {
		databaseServer.wipeDatabase();
	}

	@Test
	public void testAddingOperationsToDB() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain);

		assertEquals(0, getAmountFromDbByTable(OPERATIONS_TABLE));

		opBlockChain.getQueueOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		assertEquals(opBlockChain.getQueueOperations().size(), getAmountFromDbByTable(OPERATIONS_TABLE));
	}

	@Test
	public void testCreatingBlock() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		assertEquals(opBlock.getOperations().size(), getAmountFromDbByTable(OPERATIONS_TABLE));

		assertEquals(0, getAmountFromDbByTable(BLOCKS_TABLE));
		dbConsensusManager.insertBlock(opBlock);

		assertEquals(1, getAmountFromDbByTable(BLOCKS_TABLE));
	}

	@Test
	public void testRemoveOperation() throws FailedVerificationException {
		int amountDeletedOperations = 3;
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		Set<String> operations = new LinkedHashSet<>();
		for (int i = 0; i < amountDeletedOperations; i++) {
			operations.add(opBlock.getOperations().get(i).getRawHash());
		}

		assertEquals(amountDeletedOperations, dbConsensusManager.removeOperations(operations));
		assertEquals(opBlock.getOperations().size() - amountDeletedOperations,
				getAmountFromDbByTable(OPERATIONS_TABLE));
		assertEquals(amountDeletedOperations, getAmountFromDbByTable(OPERATIONS_TRASH_TABLE));
	}

	@Test
	public void testRemoveFullBlock() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		assertEquals(1, getAmountFromDbByTable(BLOCKS_TABLE));
		dbConsensusManager.removeFullBlock(opBlock);

		assertEquals(0, getAmountFromDbByTable(BLOCKS_TABLE));
		assertEquals(1, getAmountFromDbByTable(BLOCKS_TRASH_TABLE));
	}

	@Test
	public void testUnloadSuperblockFromDB() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateMore30Blocks(formatter, opBlockChain, dbConsensusManager, BOOTSTRAP_LIST);
		dbConsensusManager.saveMainBlockchain(opBlockChain);

		final long[] amount = new long[1];

		OpBlockChain blockChain = new OpBlockChain(opBlockChain.getParent(), opBlockChain.getBlockHeaders(0),
				dbConsensusManager.createDbAccess(
						opBlockChain.getSuperBlockHash(), opBlockChain.getSuperblockHeaders()),
				opBlockChain.getRules());

		jdbcTemplate.query("SELECT COUNT(*) FROM " + BLOCKS_TABLE + " WHERE superblock is NOT NULL", rs -> {
			amount[0] = rs.getLong(1);
		});
		assertEquals(41, amount[0]);

		dbConsensusManager.unloadSuperblockFromDB(blockChain);

		jdbcTemplate.query("SELECT COUNT(*) FROM " + BLOCKS_TABLE + " WHERE superblock is NOT NULL", rs -> {
			amount[0] = rs.getLong(1);
		});
		assertEquals(0, amount[0]);

	}

	@Test
	public void testSaveMainBlockchain() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateMore30Blocks(formatter, opBlockChain, dbConsensusManager, BOOTSTRAP_LIST);

		final long[] amount = new long[1];

		jdbcTemplate.query("SELECT COUNT(*) FROM " + BLOCKS_TABLE + " WHERE superblock is NOT NULL", rs -> {
			amount[0] = rs.getLong(1);
		});

		assertEquals(0, amount[0]);

		dbConsensusManager.saveMainBlockchain(opBlockChain);

		jdbcTemplate.query("SELECT COUNT(*) FROM " + BLOCKS_TABLE + " WHERE superblock is NOT NULL", rs -> {
			amount[0] = rs.getLong(1);
		});

		assertEquals(41, amount[0]);
	}

	@Test
	public void testCompact() throws FailedVerificationException {
		int i = 0;
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain);

		Deque<OpOperation> dequeOperations = opBlockChain.getQueueOperations();
		assertFalse(dequeOperations.isEmpty());

		Set<String> operationsToDelete = new HashSet<>();
		List<OpOperation> addOperation = new ArrayList<>();

		Iterator<OpOperation> iterator = dequeOperations.descendingIterator();
		while (i < 3) {
			OpOperation opOperation = iterator.next();
			operationsToDelete.add(opOperation.getRawHash());
			addOperation.add(opOperation);
			i++;
		}

		opBlockChain.removeQueueOperations(operationsToDelete);

		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		assertEquals(opBlock.getOperations().size(), getAmountFromDbByTable(OPERATIONS_TABLE));

		OpBlockChain newBlc = new OpBlockChain(opBlockChain, opBlockChain.getRules());
		addOperation.forEach(newBlc::addOperation);
		opBlock = newBlc.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		newBlc = dbConsensusManager.compact(0, newBlc, true);
		assertNotNull(newBlc);
	}

	@Test
	public void testGetOperationByHash() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		OpOperation opOperation = opBlock.getOperations().get(0);

		assertNotNull(dbConsensusManager.getOperationByHash(opOperation.getHash()));
		assertEquals(1,
				dbConsensusManager.removeOperations(new HashSet<>(Collections.singletonList(opOperation.getHash()))));
		assertNull(dbConsensusManager.getOperationByHash(opOperation.getHash()));
	}

	@Test
	public void testValidateExistingOperation() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		OpOperation opOperation = opBlock.getOperations().get(0);

		assertTrue(dbConsensusManager.validateExistingOperation(opOperation));
		assertEquals(1,
				dbConsensusManager.removeOperations(new HashSet<>(Collections.singletonList(opOperation.getHash()))));
		assertFalse(dbConsensusManager.validateExistingOperation(opOperation));
	}

	private long getAmountFromDbByTable(String table) {
		final long[] amount = new long[1];

		jdbcTemplate.query("SELECT COUNT (*) FROM " + table, rs -> {
			amount[0] = rs.getLong(1);
		});

		return amount[0];
	}


}