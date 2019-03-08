package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

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
	public static final String F_VALIDATION = "validation";

	public static SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
	{
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	protected Map<String, Object> fields = new TreeMap<>();
	protected transient OpOperation operation;
	
	public OpObject() {}
	
	public OpObject(OpOperation operation, Map<String, Object> fields) {
		this.operation = operation;
		this.fields.putAll(fields);
	}
	
	public OpObject(OpObject cp) {
		this.operation = cp.operation;
		this.fields.putAll(cp.fields);
	}
	
	public List<String> getId() {
		return getStringList(F_ID);
	}
	
	public void setId(String id) {
		addOrSetStringValue(F_ID, id);;
	}
	
	public void setId(String id, String id2) {
		List<String> list = new ArrayList<String>();
		list.add(id);
		list.add(id2);
		fields.put(F_ID, list);
	}
	
	public OpOperation getOperation() {
		return operation;
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
		if(value == null) {
			fields.remove(key);
		} else {
			fields.put(key, value);
		}
	}
	
	public Object remove(String key) {
		return fields.remove(key);
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		result = prime * result + ((operation == null) ? 0 : operation.hashCode());
		return result;
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
		if (operation == null) {
			if (other.operation != null)
				return false;
		} else if (!operation.equals(other.operation))
			return false;
		return true;
	}

	public static class OpObjectAdapter implements JsonDeserializer<OpObject>,
			JsonSerializer<OpObject> {
		
		@Override
		public OpObject deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			OpObject bn = new OpObject();
			bn.fields = context.deserialize(json, TreeMap.class); 
			return bn;
		}

		@Override
		public JsonElement serialize(OpObject src, Type typeOfSrc, JsonSerializationContext context) {
			return context.serialize(src.fields);
		}


	}


}
