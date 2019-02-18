package org.openplacereviews.opendb.ops.db;


import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.service.OperationsRegistry;
import org.springframework.jdbc.core.JdbcTemplate;


@OpenDBOperation(type=OperationsRegistry.OP_TYPE_SYS, name=CreateTableOperation.OP_ID)
public class CreateTableOperation implements OpenDBOperationExec {

	protected static final Log LOGGER = LogFactory.getLog(CreateTableOperation.class);
	public static final String OP_ID = "create_table";
	
	private static final String FIELD_TABLE_NAME = "name";
	private static final String FIELD_TABLE_COLUMNS = "table_columns";
	private OpDefinitionBean definition;
	private String tableName;
	private Map<String, String> tableColumns;
	
	
	@Override
	public boolean prepare(OpDefinitionBean definition) {
		this.definition = definition;
		tableName = definition.getStringValue(FIELD_TABLE_NAME);
		StringBuilder errorMessage = new StringBuilder();
		if(!OUtils.validateSqlIdentifier(tableName, errorMessage, FIELD_TABLE_NAME, "create table")) {
			throw new IllegalArgumentException(errorMessage.toString());
		}
		tableColumns = definition.getStringMap(FIELD_TABLE_COLUMNS);
		if(tableColumns == null || tableColumns.isEmpty()) {
			throw new IllegalArgumentException(String.format("Field '%s' is not specified which is necessary to create table", FIELD_TABLE_COLUMNS));
		}
		for(String col: tableColumns.keySet()) {
			if(!OUtils.validateSqlIdentifier(col, errorMessage, FIELD_TABLE_COLUMNS, "create table")) {
				throw new IllegalArgumentException(errorMessage.toString());
			}	
		}
		return true;
	}
	
	

	@Override
	public String getDescription() {
		return "This operation creates table in DB. Supported fields:"+
				"<br>'table_columns' : map of 'name' and 'type' " +
				"<br>'comment' : comment for table " +
				"<br>'name' : table name";
	}

	@Override
	public boolean execute(JdbcTemplate template) {
		StringBuilder sql = new StringBuilder("create table " + tableName);
		StringBuilder columnsDef = new StringBuilder();
		for(Entry<String, String> e : this.tableColumns.entrySet()) {
			if(columnsDef.length() > 0) {
				columnsDef.append(", ");
			}
			columnsDef.append(e.getKey()).append(" ").append(e.getValue());
		}
		sql.append("(").append(columnsDef).append(")");
		try {
			LOGGER.info("DDL executed: " + sql);
//			template.execute(sql.toString());
			
		} catch(RuntimeException e) {
			LOGGER.warn("DDL failed: " + e.getMessage(), e);
			throw e;
		}
		return true;
	}
	
	@Override
	public OpDefinitionBean getDefinition() {
		return definition;
	}
	


}
