package org.openplacereviews.opendb.ops;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.openplacereviews.opendb.ops.OpBlockChain.SearchType;
import org.openplacereviews.opendb.service.DBSchemaManager.ColumnDef;
import org.openplacereviews.opendb.util.JsonObjectUtils;
import org.openplacereviews.opendb.util.OUtils;

public class OpIndexColumn {
	private final String indexId;
	private final String opType;
	private final ColumnDef columnDef;
	private List<String> fieldsExpression = Collections.emptyList();
	
	
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
	
	public ColumnDef getColumnDef() {
		return columnDef;
	}
	
	public Object evalDBValue(OpObject opObject) {
		List<Object> array = JsonObjectUtils.getIndexObjectByField(opObject.getRawOtherFields(), 
				fieldsExpression, null);
		if(array != null) {
			Iterator<Object> it = array.iterator();
			while(it.hasNext()) {
				Object o = it.next();
				if(o == null) {
					it.remove();
				}
			}
		}
		if(array == null || array.size() == 0) {
			return null;
		}
		if(columnDef.isArray()) {
			return array.toArray(new Object[array.size()]);
		} else {
			return array.get(0);
		}
	}

	public boolean accept(OpObject opObject, SearchType searchType, Object[] argsToSearch) {
		List<Object> array = JsonObjectUtils.getIndexObjectByField(opObject.getRawOtherFields(), 
				fieldsExpression, null);
		if (array != null && argsToSearch.length > 0) {
			for (Object s : array) {
				if(searchType == SearchType.STRING_EQUALS) {
					if(OUtils.equalsStringValue(s, argsToSearch[0])) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	

	
}