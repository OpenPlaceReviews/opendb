package org.openplacereviews.opendb.ops;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpBlockChain.SearchType;
import org.openplacereviews.opendb.ops.OpPrivateObjectInstancesById.CacheObject;
import org.openplacereviews.opendb.ops.de.ColumnDef;
import org.openplacereviews.opendb.ops.de.ColumnDef.IndexType;
import org.openplacereviews.opendb.util.JsonObjectUtils;
import org.openplacereviews.opendb.util.OUtils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class OpIndexColumn {

	protected static final Log LOGGER = LogFactory.getLog(OpIndexColumn.class);

	private final String indexId;
	private final String opType;
	private final ColumnDef columnDef;
	private List<List<String>> fieldsExpression = Collections.emptyList();
	private boolean cache = true;
	
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
	
	public void setFieldsExpression(Collection<String> fieldsExpression) {
		List<List<String>> nf = new ArrayList<>();
		if (fieldsExpression != null) {
			for (String o : fieldsExpression) {
				String[] split = o.split("\\.");
				List<String> fld = new ArrayList<String>();
				for(String s : split) {
					fld.add(s);
				}
				nf.add(fld);
			}
		}
		this.fieldsExpression = nf;
	}
	
	public ColumnDef getColumnDef() {
		return columnDef;
	}
	
	public Object evalDBValue(OpObject opObject, Connection conn) {
		List<Object> array = null;
		for (List<String> f : fieldsExpression) {
			array = JsonObjectUtils.getIndexObjectByField(opObject.getRawOtherFields(), f, null);
		}
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
		// here we can convert to native db type
		if(columnDef.isArray()) {
			try {
				Array ar = conn.createArrayOf(columnDef.getScalarType(), array.toArray(new Object[array.size()]));
				return ar;
			} catch (SQLException e) {
				LOGGER.error("Error while creating sql array", e);
				return null;
			}
		} else {
			return array.get(0);
		}
	}

	public Object toNativeType(Object o) {
		if(columnDef.isInteger()) {
			return Long.parseLong(o.toString());
		}
		return o.toString();
	}
	
	Object[] getDbCondition(ObjectsSearchRequest request, Object... args) {
		Object[] o = new Object[2];
		if (request.searchType != SearchType.EQUALS) {
			throw new UnsupportedOperationException();
		}
		o[0] = columnDef.getColName() + " = ?";
		if(columnDef.getIndex() == IndexType.GIN || columnDef.getIndex() == IndexType.GIST) {
			o[0] = columnDef.getColName() + " @> ARRAY[?]";
		}
		o[1] = toNativeType(args[0]);
		return o;
	}


	@SuppressWarnings("rawtypes")
	void retrieveObjects(OpPrivateObjectInstancesById oi, ObjectsSearchRequest request, Object[] argsToSearch) {
		// only 1 arg is supported
		Object nt = toNativeType(argsToSearch[0]);
		CacheObject co = oi.getIndexCacheObject(this);
		if(co != null && co.cacheObject instanceof Set) {
			if(!((Set)co.cacheObject).contains(nt)) {
				return;
			}
		}
		
	}
	
	boolean accept(OpObject opObject, ObjectsSearchRequest request, Object[] argsToSearch) {
		List<Object> array = null;
		for (List<String> f : fieldsExpression) {
			array = JsonObjectUtils.getIndexObjectByField(opObject.getRawOtherFields(), f, null);
		}
		if (array != null && argsToSearch.length > 0) {
			for (Object s : array) {
				if(request.searchType == SearchType.EQUALS) {
					if(OUtils.equalsStringValue(s, argsToSearch[0])) {
						return true;
					}
				}
			}
		}
		return false;
	}

	
	
}