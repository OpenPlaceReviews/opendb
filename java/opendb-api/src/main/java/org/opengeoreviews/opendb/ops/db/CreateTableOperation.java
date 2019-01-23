package org.opengeoreviews.opendb.ops.db;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opengeoreviews.opendb.ops.IOpenDBOperation;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.opengeoreviews.opendb.ops.OpenDBOperation;
import org.springframework.jdbc.core.JdbcTemplate;

import wiremock.com.jayway.jsonpath.internal.Utils;

@OpenDBOperation(CreateTableOperation.OP_ID)
public class CreateTableOperation implements IOpenDBOperation {

	protected static final Log LOGGER = LogFactory.getLog(CreateTableOperation.class);
	
	public static final String OP_ID = "create_table";
	
	private static final String FIELD_TABLE_NAME = "table_name";
	private static final String FIELD_TABLE_COLUMNS = "table_columns";
	private OpDefinitionBean definition;
	private String tableName;
	private Map<String, String> tableColumns;
	
	
	
	@Override
	public String getName() {
		return OP_ID;
	}
	
	

	@Override
	public boolean prepare(OpDefinitionBean definition, StringBuilder errorMessage) {
		this.definition = definition;
		tableName = definition.getStringValue(FIELD_TABLE_NAME);
		if(Utils.isEmpty(tableName)) {
			errorMessage.append(String.format("Field '%s' is not specified which is necessary to create table", FIELD_TABLE_NAME));
			return false;
		}
		tableColumns = definition.getStringMap(FIELD_TABLE_COLUMNS);
		if(tableColumns == null || tableColumns.isEmpty()) {
			errorMessage.append(String.format("Field '%s' is not specified which is necessary to create table", FIELD_TABLE_COLUMNS));
			return false;
		}
		return true;
	}
	
	

	@Override
	public String getDescription() {
		return "This operation creates table in DB. Supported fields:"+
				"<br>'table_columns' : map of 'name' and 'type' " +
				"<br>'comment' : comment for table " +
				"<br>'table_name' : table name";
	}

	@Override
	public boolean execute(JdbcTemplate template, StringBuilder errorMessage) {
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
			template.execute(sql.toString());
		} catch(RuntimeException e) {
			LOGGER.warn("DDL failed: " + e.getMessage(), e);
			errorMessage.append("Failed to execute DDL: " + e.getMessage());
			return false;
		}
		return true;
	}
	
	@Override
	public OpDefinitionBean getDefinition() {
		return definition;
	}
	

	@Override
	public String getApprovalType() {
		// TODO Auto-generated method stub
		return null;
	}


}
