package org.openplacereviews.opendb.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.ValidationTimer;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OpExprEvaluator;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class DBDataManager {
	protected static final Log LOGGER = LogFactory.getLog(DBDataManager.class);
	
	private static final String FIELD_NAME = "name";
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	private LogOperationService logSystem;

	private Map<String, TableDefinition> tableDefinitions = new TreeMap<>();
	
	private Map<String, List<TableMapping>> opTableMappings = new TreeMap<>();	
	
	
	//////////// SYSTEM TABLES DDL ////////////
	protected static final String DDL_CREATE_TABLE_BLOCKS = "create table blocks (hash bytea, phash bytea, blockid int, details jsonb)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_HASH = "create index blocks_hash_ind on blocks(hash)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_PHASH = "create index blocks_phash_ind on blocks(phash)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_BLOCKID = "create index blocks_blockid_ind on blocks(blockid)";
	
	
	protected static final String DDL_CREATE_TABLE_OPS = "create table operations (hash bytea, details jsonb)";
	protected static final String DDL_CREATE_TABLE_OPS_INDEX_HASH = "create index operations_hash_ind on operations(hash)";
	// 
	
	
	public static void main(String[] args) {
		System.out.println(DDL_CREATE_TABLE_BLOCKS + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_HASH + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_PHASH + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_BLOCKID + ';');
		
		System.out.println(DDL_CREATE_TABLE_OPS + ';');
		System.out.println(DDL_CREATE_TABLE_OPS_INDEX_HASH + ';');
	}
	
	protected static class ColumnMapping {
		String name;
		SqlColumnType type;
		OpExprEvaluator expression;
	}
	
	protected static class TableMapping {
		List<ColumnMapping> columnMappings = new ArrayList<>();
		String preparedStatement;
	}
	
	protected static class TableDefinition {
		OpOperation def; 
		String identityField;
	}
	
	public enum SqlColumnType {
		TEXT,
		INT,
		TIMESTAMP,
		JSONB
	}
	
	public OpBlockChain init(MetadataDb metadataDB) {
		LOGGER.info("... Loading blocks ...");
		ValidationTimer timer = new ValidationTimer();
		timer.start();
		OpBlockChain blc = new OpBlockChain(null);
		OpBlockchainRules rules = new OpBlockchainRules(formatter, logSystem);
		List<OpBlock> blocks = 
				jdbcTemplate.query("SELECT blockId, details from blocks order by blockId asc", new RowMapper<OpBlock>() {

			@Override
			public OpBlock mapRow(ResultSet rs, int rowNum) throws SQLException {
				
				return formatter.parseBlock(rs.getString(2));
			}
			
		});
		LOGGER.info(String.format("... Loaded %d blocks ...", blocks.size()));
		int blockId = 0;
		Set<String> addedOperations = new TreeSet<String>(); 
		for (OpBlock b : blocks) {
			for (OpOperation o : b.getOperations()) {
				if (!blc.addOperation(o, rules)) {
					throw new IllegalStateException("Could not add operation: " + formatter.toJson(o));
				}
				addedOperations.add(o.getRawHash());
			}
			blc.replicateBlock(b, rules, timer);
			LOGGER.info(String.format("... Initialized block %d ...", blockId++));
		}
		LOGGER.info("... Loading operation queue ! TODO !  ...");
		LOGGER.info(String.format("+++ Database mapping is inititialized. Loaded %d table definitions, %d mappings", 
				tableDefinitions.size(), opTableMappings.size()));
		return blc;
	}
	
	public void insertBlock(OpBlock opBlock) {
		PGobject pGobject = new PGobject();
		pGobject.setType("jsonb");
		try {
			pGobject.setValue(formatter.toJson(opBlock));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		jdbcTemplate.update("INSERT INTO blocks(hash, phash, blockid, details) VALUES (?, ?, ?, ?)" , 
				SecUtils.getHashBytes(opBlock.getHash()), SecUtils.getHashBytes(opBlock.getStringValue(OpBlock.F_PREV_BLOCK_HASH)),
				opBlock.getBlockId(), pGobject);
	}
	
	public void insertOperation(OpOperation op) {
		PGobject pGobject = new PGobject();
		pGobject.setType("jsonb");
		try {
			pGobject.setValue(formatter.toJson(op));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		jdbcTemplate.update("INSERT INTO operations(hash, details) VALUES (?, ?)" , 
				SecUtils.getHashBytes(op.getHash()), pGobject);
	}
	

	protected boolean createTable(OpOperation definition) {
		String tableName = definition.getStringValue(FIELD_NAME);
		Map<String, String> tableColumns = new TreeMap<String, String>();
		StringBuilder sql = new StringBuilder("create table " + tableName);
		StringBuilder columnsDef = new StringBuilder();
		for(Entry<String, String> e : tableColumns.entrySet()) {
			if(columnsDef.length() > 0) {
				columnsDef.append(", ");
			}
			columnsDef.append(e.getKey()).append(" ").append(e.getValue());
		}
		sql.append("(").append(columnsDef).append(")");
		try {
			LOGGER.info("DDL executed: " + sql);
			jdbcTemplate.execute(sql.toString());
		} catch(RuntimeException e) {
			LOGGER.warn("DDL failed: " + e.getMessage(), e);
			throw e;
		}
		return true;
	}
	
	
	protected SqlColumnType getSqlType(String colType) {
		if(colType != null) {
			if(colType.contains("int") || colType.contains("serial")) {
				return SqlColumnType.INT;
			}
			if(colType.contains("jsonb")) {
				return SqlColumnType.JSONB;
			}
			if(colType.contains("timestamp")) {
				return SqlColumnType.TIMESTAMP;
			}
		}
		return SqlColumnType.TEXT;
	}

	



}
