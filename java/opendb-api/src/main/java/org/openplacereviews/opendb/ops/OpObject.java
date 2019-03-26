package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.openplacereviews.opendb.OUtils;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OpObject {
	
	public static final String F_NAME = "name";
	public static final String F_ID = "id";
	public static final String F_COMMENT = "comment";
	public static final String TYPE_OP = "sys.op";
	public static final String TYPE_BLOCK = "sys.block";
	public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	// transient info about validation timing etc
	public static final String F_EVAL = "eval";
	public static final String F_VALIDATION = "validation";

	public static SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
	{
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	protected Map<String, Object> fields = new TreeMap<>();
	protected transient Map<String, Object> cacheFields;
	protected boolean isImmutable;
	
	protected transient String parentType;
	protected transient String parentHash;
	
	public OpObject() {}
	
	public OpObject(OpObject cp) {
		this(cp, false);
	}
	
	public OpObject(OpObject cp, boolean copyCacheFields) {
		this.fields.putAll(cp.fields);
		if(copyCacheFields && cp.cacheFields != null) {
			this.cacheFields = new ConcurrentHashMap<String, Object>();
			this.cacheFields.putAll(cp.cacheFields);
		}
	}
	
	public void setParentOp(OpOperation op) {
		setParentOp(op.type, op.getRawHash());
	}
	
	public void setParentOp(String parentType, String parentHash) {
		this.parentType = parentType;
		this.parentHash = parentHash;
	}
	
	public String getParentHash() {
		return parentHash;
	}
	
	public String getParentType() {
		return parentType;
	}
	
	public List<String> getId() {
		return getStringList(F_ID);
	}
	
	public void setId(String id) {
		addOrSetStringValue(F_ID, id);;
	}
	
	public boolean isImmutable() {
		return isImmutable;
	}
	
	public OpObject makeImmutable() {
		isImmutable = true;
		return this;
	}
	
	public Object getCacheObject(String f) {
		if(cacheFields == null) {
			return null;
		}
		return cacheFields.get(f);
	}
	
	public void putCacheObject(String f, Object o) {
		if (isImmutable()) {
			if (cacheFields == null) {
				cacheFields = new ConcurrentHashMap<String, Object>();
			}
			cacheFields.put(f, o);
		}
	}
	
	public void setId(String id, String id2) {
		List<String> list = new ArrayList<String>();
		list.add(id);
		list.add(id2);
		putObjectValue(F_ID, list);
	}
	
	public String getName() {
		return getStringValue(F_NAME);
	}
	
	public String getComment() {
		return getStringValue(F_COMMENT);
	}
	
	public Map<String, Object> getRawOtherFields() {
		return fields;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, String> getStringMap(String field) {
		return (Map<String, String>) fields.get(field);
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, List<String>> getMapStringList(String field) {
		return (Map<String, List<String>>) fields.get(field);
	}
	
	@SuppressWarnings("unchecked")
	public List<Map<String, String>> getListStringMap(String field) {
		return (List<Map<String, String>>) fields.get(field);
	}
	
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> getListStringObjMap(String field) {
		return (List<Map<String, Object>>) fields.get(field);
	}
	

	public long getDate(String field) {
		String date = getStringValue(field);
		if(OUtils.isEmpty(date)) {
			return 0;
		}
		try {
			return dateFormat.parse(date).getTime();
		} catch (ParseException e) {
			return 0;
		}
	}

	
	public void setDate(String field, long time) {
		putStringValue(field, dateFormat.format(new Date(time)));
	}
	
	public Number getNumberValue(String field) {
		return (Number) fields.get(field);
	}
	
	public int getIntValue(String key, int def) {
		Number o = getNumberValue(key);
		return o == null ? def : o.intValue();
	}
	
	public long getLongValue(String key, long def) {
		Number o = getNumberValue(key);
		return o == null ? def : o.longValue();
	}
	
	public String getStringValue(String field) {
		Object o = fields.get(field);
		if (o instanceof String || o == null) {
			return (String) o;
		}
		return o.toString();
	}
	
	@SuppressWarnings("unchecked")
	public List<String> getStringList(String field) {
		// cast to list if it is single value
		Object o = fields.get(field);
		if(o == null || o.toString().isEmpty()) {
			return Collections.emptyList();
		}
		if(o instanceof String) {
			return Collections.singletonList(o.toString());
		}
		return (List<String>) o;
	}
	
	public void putStringValue(String key, String value) {
		checkNotImmutable();
		if(value == null) {
			fields.remove(key);
		} else {
			fields.put(key, value);
		}
	}
	
	/**
	 * Operates as a single value if cardinality is less than 1
	 * or as a list of values if it stores > 1 value
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	public void addOrSetStringValue(String key, String value) {
		checkNotImmutable();
		Object o = fields.get(key);
		if(o == null) {
			fields.put(key, value);
		} else if(o instanceof List) {
			((List<String>) o).add(value);
		} else  {
			List<String> list = new ArrayList<String>();
			list.add(o.toString());
			list.add(value);
			fields.put(key, list);
		}
	}
	
	public void putObjectValue(String key, Object value) {
		checkNotImmutable();
		if(value == null) {
			fields.remove(key);
		} else {
			fields.put(key, value);
		}
	}
	
	public void checkNotImmutable() {
		if(isImmutable) {
			throw new IllegalStateException("Object is immutable");
		}
		
	}
	
	public void checkImmutable() {
		if(!isImmutable) {
			throw new IllegalStateException("Object is mutable");
		}
	}

	public Object remove(String key) {
		checkNotImmutable();
		return fields.remove(key);
	}
	
	public Map<String, Object> getMixedFieldsAndCacheMap() {
		TreeMap<String, Object> mp = new TreeMap<>(fields);
		if(cacheFields != null) {
			TreeMap<String, Object> eval = new TreeMap<String, Object>();
			Iterator<Entry<String, Object>> it = cacheFields.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, Object> e = it.next();
				Object v = e.getValue();
				if(v instanceof Map || v instanceof String || v instanceof Number) {
					eval.put(e.getKey(), v);
				}
			}
			if(eval.size() > 0) {
				mp.put(F_EVAL, eval);
			}
		}
		return mp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		return result;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + fields + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OpObject other = (OpObject) obj;
		if (fields == null) {
			if (other.fields != null)
				return false;
		} else if (!fields.equals(other.fields))
			return false;
		return true;
	}

	public static class OpObjectAdapter implements JsonDeserializer<OpObject>,
			JsonSerializer<OpObject> {
		
		private boolean fullOutput;

		public OpObjectAdapter(boolean fullOutput) {
			this.fullOutput = fullOutput;
		}

		@Override
		public OpObject deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			OpObject bn = new OpObject();
			bn.fields = context.deserialize(json, TreeMap.class);
			// remove cache
			bn.fields.remove(F_EVAL);
			return bn;
		}

		@Override
		public JsonElement serialize(OpObject src, Type typeOfSrc, JsonSerializationContext context) {
			return context.serialize(fullOutput ? src.getMixedFieldsAndCacheMap() : src.fields);
		}


	}


}
