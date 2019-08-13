package org.openplacereviews.opendb.ops;

import java.util.Collections;
import java.util.List;

import org.openplacereviews.opendb.service.DBSchemaManager.ColumnDef;

public class OpIndexColumn {
	private final String indexId;
	private final String opType;
	private final ColumnDef columnDef;
	private List<String> fieldsExpression = Collections.emptyList();
	
	
	// 
	String tableName;
	String colName;
	String colType;
	IndexType index;
	
	
	
	public OpIndexColumn(String opType, String indexId, ColumnDef columnDef) {
		this.opType = opType;
		this.indexId = indexId;
		this.columnDef = columnDef;
	}
	
	public String getOpType() {
		return opType;
	}
	
	public String getIndexId() {
		return indexId;
	}
	
	public void setFieldsExpression(List<String> fieldsExpression) {
		this.fieldsExpression = fieldsExpression;
	}
	
	public List<String> getFieldsExpression() {
		return fieldsExpression;
	}
	
	public String getTableName() {
		return columnDef.tableName;
	}
	
	public ColumnDef getColumnDef() {
		return columnDef;
	}
	
	
	// public final String column;
	// public final String table;
	// public final ColumnDef columnDef;
	// public final List<String> field;
	// private final String type;
	// private final String hash;
	// private final String content;

	// SCHEMA DEFINITION
	public enum IndexType {
		NOT_INDEXED, INDEXED, GIN, GIST
	}




	
}