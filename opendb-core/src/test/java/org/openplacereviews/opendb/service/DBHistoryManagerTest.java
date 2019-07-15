package org.openplacereviews.opendb.service;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.psql.PostgreSQLServer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateMetadataDB;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.service.DBSchemaManager.OP_OBJ_HISTORY_TABLE;

public class DBHistoryManagerTest {

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
	private JdbcTemplate jdbcTemplate;
	private OpenDBServer.MetadataDb metadataDb;

	@After
	public void tearDown() throws Exception {
		databaseServer.wipeDatabase();
	}

	@Test
	public void generatingHistoryForObjectFromDB() throws SQLException, FailedVerificationException {
		generateDBConnection();
		OpBlockChain opBlockChain = dbConsensusManager.init(metadataDb);
		blocksManager.init(metadataDb, opBlockChain);
		generateOperations(formatter, opBlockChain);

		OpBlock opBlock = opBlockChain.createBlock(serverName, serverKeyPair);
		historyManager.saveHistoryForBlockOperations(opBlock, null);

		assertEquals(3, getAmountFromHistoryTableByObj("osm.place", Arrays.asList("12345662")));

		HistoryManager.HistoryObjectRequest historyObjectRequest = new HistoryManager.HistoryObjectRequest(
				HistoryManager.HISTORY_BY_OBJECT,
				Arrays.asList("12345662"),
				20,
				HistoryManager.DESC_SORT
		);
		historyManager.retrieveHistory(historyObjectRequest);

		List<HistoryManager.HistoryEdit> historyObject = historyObjectRequest.historySearchResult.get(Arrays.asList("osm.place","12345662"));
		assertEquals(3, historyObject.size());

	}

	private void generateDBConnection() throws SQLException {
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
		Mockito.doNothing().when(fileBackupManager).init();

		Mockito.doCallRealMethod().when(dbSchemaManager).initializeDatabaseSchema(metadataDb, jdbcTemplate);
		Mockito.doCallRealMethod().when(dbConsensusManager).insertBlock(any());

		metadataDb = new OpenDBServer.MetadataDb();
		generateMetadataDB(metadataDb, jdbcTemplate);
	}

	private long getAmountFromHistoryTableByObj(String type, List<String> id) {
		final long[] amount = new long[1];

		Object[] args = new Object[1 + id.size()];
		for (int i = 0; i < id.size(); i++) {
			args[i] = id.get(i);
		}
		args[id.size()] = type;
		jdbcTemplate.query("SELECT COUNT (*) FROM " + OP_OBJ_HISTORY_TABLE + " WHERE " +
				dbSchemaManager.generatePKString(OP_OBJ_HISTORY_TABLE, "p%1$d = ?", ",", id.size()) + " AND type = ?", rs -> {
			amount[0] = rs.getLong(1);
		}, args);

		return amount[0];
	}

}
