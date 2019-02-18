package org.openplacereviews.opendb.service;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

@Service
public class DBDataManager {
	protected static final Log LOGGER = LogFactory.getLog(DBDataManager.class);
	
	private static final String FIELD_NAME = "name";
	private static final String FIELD_TABLE_NAME = "table";
	private static final String FIELD_TABLE_COLUMNS = "table_columns";
	
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private Map<String, OpDefinitionBean> tableDefinitions = new TreeMap<>();
	
	private Map<String, List<TableMapping>> opTableMappings = new TreeMap<>();	
	
	protected static class SimpleExpressionEvaluator {
		List<String> fieldAccess = new ArrayList<String>();

		public Object execute(SqlColumnType type, JsonObject obj) {
			JsonElement o = obj;
			for(String f : fieldAccess) {
				o = o.getAsJsonObject().get(f);
				if(o == null) {
					break;
				}
			}
			if (o == null) {
				return null;
			}
			if(type == SqlColumnType.INT) {
				return o.getAsInt();
			}
			if(type == SqlColumnType.TIMESTAMP) {
				try {
					return OpBlock.dateFormat.parse(o.getAsString());
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}
			if (type == SqlColumnType.JSONB) {
				PGobject jsonObject = new PGobject();
				jsonObject.setType("json");
				try {
					jsonObject.setValue(o.toString());
				} catch (SQLException e) {
					throw new IllegalArgumentException(e);
				}
				return jsonObject;
			}
			return o.toString();
		}
	}
	
	protected static class ColumnMapping {
		String name;
		SqlColumnType type;
		SimpleExpressionEvaluator expression;
	}
	
	protected static class TableMapping {
		List<ColumnMapping> columnMappings = new ArrayList<>();
		String preparedStatement;
	}
	
	private enum SqlColumnType {
		TEXT,
		INT,
		TIMESTAMP,
		JSONB
	}
	
	
	public boolean registerTableDefinition(OpDefinitionBean definition) {
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
		createTable(definition);
		tableDefinitions.put(tableName, definition);
		return true;
	}
	
	public void registerMappingOperation(String operationId, OpDefinitionBean def) {
		String tableName = def.getStringValue(FIELD_TABLE_NAME);
		if(!OUtils.isEmpty(tableName)) {
			OpDefinitionBean tableDef = tableDefinitions.get(tableName);
			if(tableDef == null) {
				throw new IllegalArgumentException(String.format("Mapping can't be registered cause the table '%s' is not registered", tableName));
			}
			StringBuilder inSql = new StringBuilder();
			StringBuilder values = new StringBuilder();
			inSql.append("INSERT INTO ").append(tableName).append("(");
			Map<String, String> tCols = def.getStringMap(FIELD_TABLE_COLUMNS);
			TableMapping tableMapping = new TableMapping();
			for(Entry<String, String> e : tCols.entrySet()) {
				String k = e.getKey();	
				if(values.length() != 0) {
					inSql.append(", ");
					values.append(", ");
					
				}
				inSql.append(k);
				ColumnMapping cm = new ColumnMapping();
				cm.type = getSqlType(tableDef, k);
				cm.expression = prepareMappingExpression(e.getValue());
				cm.name = k;
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
	
	
	private SimpleExpressionEvaluator prepareMappingExpression(String value) {
		SimpleExpressionEvaluator  s = new SimpleExpressionEvaluator();
		if(value.equals("this")) {
		} else if (value.startsWith(".") || value.startsWith("this.")) {
			String[] fields = value.substring(value.indexOf('.') + 1).split("\\.");
			for (String f : fields) {
				if(!OUtils.isValidJavaIdentifier(f)) {
					throw new UnsupportedOperationException(String.format("Invalid field access '%s' in expression '%s'", f, value));
				}
				s.fieldAccess.add(f);
			}
		} else {
			throw new UnsupportedOperationException();
		}
		return s;
	}

	private boolean createTable(OpDefinitionBean definition) {
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
	
	

	public void executeMappingOperation(String op, JsonObject obj) {
		List<TableMapping> tableMappings = opTableMappings.get(op);
		if(tableMappings != null) {
			for (TableMapping t : tableMappings) {
				try {
					Object[] o = new Object[t.columnMappings.size()];
					for (int i = 0; i < t.columnMappings.size(); i++) {
						ColumnMapping colMapping = t.columnMappings.get(i);
						o[i] = colMapping.expression.execute(colMapping.type, obj);
					}

					jdbcTemplate.update(t.preparedStatement, o);
				} catch (RuntimeException e) {
					LOGGER.warn("SQL failed: " + e.getMessage(), e);
					throw e;
				}
			}
		}
	}



	private SqlColumnType getSqlType(OpDefinitionBean tableDef, String column) {
		if(tableDef != null) {
			String colType = tableDef.getStringMap(FIELD_TABLE_COLUMNS).get(column);
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
		}
		return SqlColumnType.TEXT;
	}


}
