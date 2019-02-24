package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.openplacereviews.opendb.OUtils;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OpObject  {
	
	public static final String F_NAME = "name";
	public static final String F_COMMENT = "comment";
	public static final String TYPE_OP = "sys.op";
	public static final String TYPE_BLOCK = "sys.block";
	public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	public static SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
	{
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	
	protected Map<String, Object> fields = new TreeMap<>();
	protected OpOperation operation;
	
	
	public OpObject() {
	}
	
	public OpObject(OpObject cp) {
		this.fields.putAll(cp.fields);
	}
	
	public String getType() {
		return operation != null ? operation.getOperationType() : null;
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
	public List<String> getStringList(String field) {
		return (List<String>) fields.get(field);
	}
	
	@SuppressWarnings("unchecked")
	public List<Map<String, String>> getListStringMap(String field) {
		return (List<Map<String, String>>) fields.get(field);
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
	
	public String getStringValue(String field) {
		Object o = fields.get(field);
		if (o instanceof String || o == null) {
			return (String) o;
		}
		return o.toString();
	}
	
	public void putStringValue(String key, String value) {
		if(value == null) {
			fields.remove(key);
		} else {
			fields.put(key, value);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void addStringValue(String key, String value) {
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

	public static class OpObjectAdapter implements JsonDeserializer<OpObject>,
			JsonSerializer<OpObject> {
		
		@Override
		public OpObject deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			JsonObject o = json.getAsJsonObject();
			OpObject bn = new OpObject();
			bn.fields = context.deserialize(o, TreeMap.class); 
			return bn;
		}

		@Override
		public JsonElement serialize(OpObject src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject o = new JsonObject();
			// o.addProperty(F_OPERATION, src.type + ":" + src.operation);
			for(String k : src.fields.keySet()) {
				Object ob = src.fields.get(k);
				o.add(k, context.serialize(ob));
			}
			return o;
		}


	}

}
