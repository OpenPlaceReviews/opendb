package org.openplacereviews.opendb.ops;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpBlockChain.SearchType;
import org.openplacereviews.opendb.ops.OpPrivateObjectInstancesById.CacheObject;
import org.openplacereviews.opendb.ops.de.ColumnDef;
import org.openplacereviews.opendb.ops.de.ColumnDef.IndexType;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.util.JsonObjectUtils;
import org.openplacereviews.opendb.util.OUtils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class OpIndexColumn {

	protected static final Log LOGGER = LogFactory.getLog(OpIndexColumn.class);

	private final String indexId;
	private final String opType;
	private final ColumnDef columnDef;
	private List<List<String>> fieldsExpression = Collections.emptyList();
	private int cacheRuntimeBlocks = 64;
	private int cacheDBBlocks = 64;
	
	public OpIndexColumn(String opType, String indexId, ColumnDef columnDef) {
		this.opType = opType;
		this.indexId = indexId;
		this.columnDef = columnDef;
	}
	
	public void setCacheDBBlocks(int cacheDB) {
		this.cacheDBBlocks = cacheDB;
	}
	
	public void setCacheRuntimeBlocks(int cacheRuntimeBlocks) {
		this.cacheRuntimeBlocks = cacheRuntimeBlocks;
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
		List<Object> array = eval(opObject, null);
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


	public Stream<Entry<CompoundKey, OpObject>> streamObjects(OpPrivateObjectInstancesById oi, 
			int superBlockSize, String type, int limit, ObjectsSearchRequest request, Object[] args) {
		Set<Object> keys = getKeysFromCache(oi);
		if (keys == null) {
			if((oi.getDbAccess() != null && cacheDBBlocks >= superBlockSize) || 
					 (oi.getDbAccess() == null && cacheRuntimeBlocks >= superBlockSize)) {
				keys = buildCacheKeys(oi, type);
			}
		}
		if(keys != null && !keys.contains(toNativeType(args[0]))) {
			return Stream.empty();
		}
		Stream<Entry<CompoundKey, OpObject>> stream;
		if(oi.getDbAccess() != null){
			stream = oi.getDbAccess().streamObjects(type, limit, getDbCondition(request, args));
		} else {
			stream = oi.getRawObjects().entrySet().stream();
			stream = stream.filter(new Predicate<Entry<CompoundKey, OpObject>>() {
				@Override
				public boolean test(Entry<CompoundKey, OpObject> t) {
					return accept(t.getValue(), request, args);
				}
			});
			
		}
		return stream;
	}

	@SuppressWarnings("unchecked")
	private Set<Object> getKeysFromCache(OpPrivateObjectInstancesById oi) {
		int ev = oi.getEditVersion();
		CacheObject cacheObject = oi.getCacheObjectByKey(this);
		Set<Object> keys = null;
		if(cacheObject != null && cacheObject.cacheVersion == ev) {
			keys = (Set<Object>) cacheObject.cacheObject;
		}
		return keys;
	}

	private synchronized Set<Object> buildCacheKeys(OpPrivateObjectInstancesById oi, String type) {
		Set<Object> keys = getKeysFromCache(oi);
		if (keys != null) {
			return keys;
		}
		int ev = oi.getEditVersion();
		if (oi.getDbAccess() != null) {
			Iterator<Entry<CompoundKey, OpObject>> it = oi.getDbAccess().streamObjects(type, -1).iterator();
			keys = buildCacheKeys(it);
			oi.setCacheObjectByKey(this, keys, ev);
		} else if (oi.getDbAccess() == null) {
			keys = buildCacheKeys(oi.getRawObjects().entrySet().iterator());
			oi.setCacheObjectByKey(this, keys, ev);
		}
		return keys;
	}

	private Set<Object> buildCacheKeys(Iterator<Entry<CompoundKey, OpObject>> it) {
		Set<Object> keys;
		List<Object> array = new ArrayList<Object>();
		keys = new HashSet<Object>();
		while(it.hasNext()) {
			Entry<CompoundKey, OpObject> e = it.next();
			OpObject o = e.getValue();
			array.clear();
			array = eval(o, array);
			for (Object k : array) {
				keys.add(k);
			}
		}
		return keys;
	}
	
	private Object toNativeType(Object o) {
		if(columnDef.isInteger()) {
			return Long.parseLong(o.toString());
		}
		return o.toString();
	}

	private Object[] getDbCondition(ObjectsSearchRequest request, Object... args) {
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
	
	
	private boolean accept(OpObject opObject, ObjectsSearchRequest request, Object[] argsToSearch) {
		List<Object> array = eval(opObject, null);
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

	private List<Object> eval(OpObject opObject, List<Object> array ) {
		for (List<String> f : fieldsExpression) {
			array = JsonObjectUtils.getIndexObjectByField(opObject.getRawOtherFields(), f, array);
		}
		return array;
	}

	

	
	
}