package org.openplacereviews.opendb.service;

import org.junit.*;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.DBConstants;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.service.DBSchemaManager.*;

public class DBConsensusManagerTest {

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

	@Rule
	public final PostgreSQLServer.Wiper databaseWiper = new PostgreSQLServer.Wiper();

	private JdbcTemplate jdbcTemplate;
	private OpenDBServer.MetadataDb metadataDb;

	@Before
	public void beforeEachTestMethod() throws Exception {
		MockitoAnnotations.initMocks(this);

		Connection connection = databaseServer.getConnection();
		this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, false));

		ReflectionTestUtils.setField(dbConsensusManager, "jdbcTemplate", jdbcTemplate);
		ReflectionTestUtils.setField(dbConsensusManager, "dbSchema", dbSchemaManager);
		ReflectionTestUtils.setField(dbConsensusManager, "backupManager", fileBackupManager);

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());

		Mockito.doNothing().when(fileBackupManager).init();

		metadataDb = new OpenDBServer.MetadataDb();
		try {
			DatabaseMetaData mt = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection().getMetaData();
			ResultSet rs = mt.getColumns(null, DBConstants.SCHEMA_NAME, null, null);
			while(rs.next()) {
				String tName = rs.getString("TABLE_NAME");
				if(!metadataDb.tablesSpec.containsKey(tName)) {
					metadataDb.tablesSpec.put(tName, new ArrayList<>());
				}
				List<OpenDBServer.MetadataColumnSpec> cols = metadataDb.tablesSpec.get(tName);
				OpenDBServer.MetadataColumnSpec spec = new OpenDBServer.MetadataColumnSpec();
				spec.columnName = rs.getString("COLUMN_NAME");
				spec.sqlType = rs.getInt("DATA_TYPE");
				spec.dataType = rs.getString("TYPE_NAME");
				spec.columnSize = rs.getInt("COLUMN_SIZE");
				cols.add(spec);
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Can't read db metadata", e);
		}
	}

	@After
	public void tearDown() throws Exception {
		databaseServer.wipeDatabase();
	}

	@AfterClass
	public static void closeDBConnection() throws SQLException {
		databaseServer.getConnection().close();
	}

	@Test
	public void testAddingOperationsToDB() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);

		assertEquals(0, getAmountFromDbByTable(OPERATIONS_TABLE));

		opBlockChain.getQueueOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		assertEquals(opBlockChain.getQueueOperations().size(), getAmountFromDbByTable(OPERATIONS_TABLE));
	}

	@Test
	public void testCreatingBlock() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);
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

		generateOperations(formatter, opBlockChain, serverKeyPair);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		Set<String> operations = new LinkedHashSet<>();
		for(int i = 0; i < amountDeletedOperations; i++) {
			operations.add(opBlock.getOperations().get(i).getRawHash());
		}

		assertEquals(amountDeletedOperations, dbConsensusManager.removeOperations(operations));
		assertEquals(opBlock.getOperations().size() - amountDeletedOperations, getAmountFromDbByTable(OPERATIONS_TABLE));
		assertEquals(amountDeletedOperations, getAmountFromDbByTable(OPERATIONS_TRASH_TABLE));
	}

	@Test
	public void testRemoveFullBlock() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		dbConsensusManager.insertBlock(opBlock);

		assertEquals(1, getAmountFromDbByTable(BLOCKS_TABLE));
		dbConsensusManager.removeFullBlock(opBlock);

		assertEquals(0, getAmountFromDbByTable(BLOCKS_TABLE));
		assertEquals(1, getAmountFromDbByTable(BLOCKS_TRASH_TABLE));
	}

	//TODO
	@Ignore
	@Test
	public void testUnloadSuperblockFromDB() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		assertEquals(opBlock.getOperations().size(), getAmountFromDbByTable(OPERATIONS_TABLE));

		assertEquals(0, getAmountFromDbByTable(BLOCKS_TABLE));

		dbConsensusManager.saveMainBlockchain(opBlockChain);
		dbConsensusManager.insertBlock(opBlock);

		final long[] amount = new long[1];

		jdbcTemplate.query("SELECT COUNT(*) FROM " + OPERATIONS_TABLE + " WHERE superblock is NOT NULL", rs -> {
			amount[0] = rs.getLong(1);
		});
		opBlockChain = dbConsensusManager.unloadSuperblockFromDB(opBlockChain);

	}

	//TODO
	@Ignore
	@Test
	public void testSaveMainBlockchain() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		dbConsensusManager.saveMainBlockchain(opBlockChain);
	}

	//TODO
	@Ignore
	@Test
	public void testCompact() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));
		assertEquals(opBlock.getOperations().size(), getAmountFromDbByTable(OPERATIONS_TABLE));

		assertEquals(0, getAmountFromDbByTable(BLOCKS_TABLE));

		dbConsensusManager.compact(0, opBlockChain, true);
	}

	@Test
	public void testGetOperationByHash() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		OpOperation opOperation = opBlock.getOperations().get(0);

		assertNotNull(dbConsensusManager.getOperationByHash(opOperation.getHash()));
		assertEquals(1, dbConsensusManager.removeOperations(new HashSet<>(Collections.singletonList(opOperation.getHash()))));
		assertNull(dbConsensusManager.getOperationByHash(opOperation.getHash()));
	}

	@Test
	public void testValidateExistingOperation() throws FailedVerificationException {
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);

		generateOperations(formatter, opBlockChain, serverKeyPair);
		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);

		opBlock.getOperations().forEach(opOperation -> dbConsensusManager.insertOperation(opOperation));

		OpOperation opOperation = opBlock.getOperations().get(0);

		assertTrue(dbConsensusManager.validateExistingOperation(opOperation));
		assertEquals(1, dbConsensusManager.removeOperations(new HashSet<>(Collections.singletonList(opOperation.getHash()))));
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