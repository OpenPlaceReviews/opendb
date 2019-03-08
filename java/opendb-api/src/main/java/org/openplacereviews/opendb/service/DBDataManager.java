package org.openplacereviews.opendb.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.SimpleExprEvaluator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import com.google.gson.JsonObject;

@Service
public class DBDataManager {
	protected static final Log LOGGER = LogFactory.getLog(DBDataManager.class);
	
	private static final String FIELD_NAME = "name";
	private static final String FIELD_TABLE_NAME = "table";
	private static final String FIELD_TABLE_COLUMNS = "table_columns";
	private static final String FIELD_IDENTITY = "identity";
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private JsonFormatter formatter;

	private Map<String, TableDefinition> tableDefinitions = new TreeMap<>();
	
	private Map<String, List<TableMapping>> opTableMappings = new TreeMap<>();	
	
	
	protected static class ColumnMapping {
		String name;
		SqlColumnType type;
		SimpleExprEvaluator expression;
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
	
	public void init(MetadataDb metadataDB) {
		LOGGER.info("... TODO Database mapping. Loading table definitions and mappings ...");
//		System.out.println(metadataDB.tablesSpec);
//		if(metadataDB.tablesSpec.containsKey(DBConstants.TABLES_TABLE)) {
//			List<OpOperation> ops = loadOperations(DBConstants.TABLES_TABLE);
//			for(OpOperation op : ops) {
//				registerTableDefinition(op, false);
//			}
//		}
		LOGGER.info(String.format("+++ Database mapping is inititialized. Loaded %d table definitions, %d mappings", 
				tableDefinitions.size(), opTableMappings.size()));
	}
	
	public List<OpOperation> loadOperations(String table) {
		return jdbcTemplate.query("SELECT details FROM " + table, new RowMapper<OpOperation>() {

			@Override
			public OpOperation mapRow(ResultSet rs, int rowNum) throws SQLException {
				return formatter.parseOperation(rs.getString(1));
			}

		});
	}
	
	
	public boolean registerTableDefinition(OpOperation definition, boolean create) {
		String tableName = definition.getStringValue(FIELD_NAME);
		StringBuilder errorMessage = new StringBuilder();
		if(!OUtils.validateSqlIdentifier(tableName, errorMessage, FIELD_NAME, "create table")) {
			throw new IllegalArgumentException(errorMessage.toString());
		}
		Map<String, String> tableColumns = definition.getStringMap(FIELD_TABLE_COLUMNS);
		if(tableColumns == null || tableColumns.isEmpty()) {
			throw new IllegalArgumentException(String.format("Field '%s' is not specified which is necessary to create table", FIELD_TABLE_COLUMNS));
		}
		for(String col: tableColumns.keySet()) {
			if(!OUtils.validateSqlIdentifier(col, errorMessage, FIELD_TABLE_COLUMNS, "create table")) {
				throw new IllegalArgumentException(errorMessage.toString());
			}	
		}
		if(tableDefinitions.containsKey(tableName)) {
			throw new IllegalArgumentException(String.format("Table '%s' is already registered in db", tableName));
		}
		if(create) {
			createTable(definition);
		}
		TableDefinition tdf = new TableDefinition();
		tdf.def = definition;
		tableDefinitions.put(tableName, tdf);
		return true;
	}
	
	
	public JsonObject queryByIdentity(String tableName, Object val) {
		TableDefinition td = tableDefinitions.get(tableName);
		if(td != null) {
			StringBuilder inSql = new StringBuilder();
			RowMapper<JsonObject> mapper = selectFromTable(inSql, td);
			String idField = td.def.getStringValue(FIELD_IDENTITY);
			inSql.append(" WHERE ").append(idField).append(" = ? ");
			List<JsonObject> lst = jdbcTemplate.query(inSql.toString(), new Object[] {val},mapper);
			if(lst.size() != 1) {
				throw new IllegalArgumentException(String.format("Query by identity to '%s' with '%s' doesn't return unique result", tableName, val));
			}
			return lst.get(0);
			
		}
		throw new IllegalArgumentException(String.format("Table doesn't exist '%s'", tableName));
	}


	private RowMapper<JsonObject> selectFromTable(StringBuilder inSql, TableDefinition td) {
		
		StringBuilder values = new StringBuilder();
		Map<String, String> tCols = td.def.getStringMap(FIELD_TABLE_COLUMNS);
		for(String colName: tCols.keySet()) {
			if(values.length() != 0) {
				values.append(", ");
			}
			values.append(colName);
		}
		inSql.append("SELECT ").append(values).append(" FROM ").append(td.def.getStringValue(FIELD_NAME));
		
		return new RowMapper<JsonObject>() {

			@Override
			public JsonObject mapRow(ResultSet rs, int rowNum) throws SQLException {
				JsonObject ob = new JsonObject();
				int i = 1;
				for(Entry<String, String> es: tCols.entrySet()) {
					String colName = es.getKey();
					String colType = es.getValue();
					SqlColumnType sqlType = getSqlType(colType);
					if(sqlType == SqlColumnType.INT) {
						ob.addProperty(colName, rs.getLong(i));	
					} else if(sqlType == SqlColumnType.JSONB) {
						String s = rs.getString(i);
						if(s != null) {
							ob.add(colName, formatter.fromJsonToJsonObject(s));
						}
					} else {
						ob.addProperty(colName, rs.getString(i));
					}
					i++;
					
				}
				return ob;
			}
		};
	}
	
	public void registerMappingOperation(String operationId, OpOperation mappingDef) {
		String tableName = mappingDef.getStringValue(FIELD_TABLE_NAME);
		if(!OUtils.isEmpty(tableName)) {
			TableDefinition tableDef = tableDefinitions.get(tableName);
			if(tableDef == null) {
				throw new IllegalArgumentException(String.format("Mapping can't be registered cause the table '%s' is not registered", tableName));
			}
			StringBuilder inSql = new StringBuilder();
			StringBuilder values = new StringBuilder();
			inSql.append("INSERT INTO ").append(tableName).append("(");
			Map<String, String> tCols = mappingDef.getStringMap(FIELD_TABLE_COLUMNS);
			Map<String, String> tdefCols = tableDef.def.getStringMap(FIELD_TABLE_COLUMNS);
			
			TableMapping tableMapping = new TableMapping();
			for(Entry<String, String> e : tCols.entrySet()) {
				String colName = e.getKey();
				String expr = e.getValue();
				if(values.length() != 0) {
					inSql.append(", ");
					values.append(", ");
					
				}
				inSql.append(colName);
				ColumnMapping cm = new ColumnMapping();
				String colType = tdefCols.get(colName);
				if(colType == null) {
					throw new IllegalArgumentException(
							String.format("Mapping can't be registered cause the column '%s' : '%s' is not registered", tableName, colName));
				}
				
				cm.type = getSqlType(colType);
				cm.expression = SimpleExprEvaluator.parseMappingExpression(expr);
				cm.name = colName;
				tableMapping.columnMappings.add(cm);
				values.append("?");
			}
			inSql.append(") VALUES (").append(values).append(")");
			if(!opTableMappings.containsKey(operationId)) {
				opTableMappings.put(operationId, new ArrayList<DBDataManager.TableMapping>());
			}
			tableMapping.preparedStatement = inSql.toString();
			opTableMappings.get(operationId).add(tableMapping);
			LOGGER.info(String.format("Mapping '%s' is registered with insert sql: %s", operationId, inSql.toString()));
		}
	}
	
	

	private boolean createTable(OpOperation definition) {
		String tableName = definition.getStringValue(FIELD_NAME);
		Map<String, String> tableColumns = definition.getStringMap(FIELD_TABLE_COLUMNS);
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
	
	
	private SqlColumnType getSqlType(String colType) {
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
