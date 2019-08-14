package org.openplacereviews.opendb.ops.de;

public class ColumnDef {
	String tableName;
	String colName;
	String colType;
	IndexType index;
	
	public ColumnDef(String tableName, String colName, String colType, IndexType index) {
		super();
		this.tableName = tableName;
		this.colName = colName;
		this.colType = colType;
		this.index = index;
	}
	
	public String getTableName() {
		return tableName;
	}

	public String getColName() {
		return colName;
	}

	public String getColType() {
		return colType;
	}

	public IndexType getIndex() {
		return index;
	}

	public boolean isArray() {
		return colType.endsWith("[]");
	}

	public enum IndexType {
		NOT_INDEXED, INDEXED, GIN, GIST
	}
}