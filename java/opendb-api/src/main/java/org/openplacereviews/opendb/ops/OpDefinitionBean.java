package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OpDefinitionBean {
	
	public static final String F_HASH = "hash";
	public static final String F_SIGNATURE = "signature";
	public static final String F_SIGNED_BY = "signed_by";
	
	private String type;
	private String operation;
	private String signedBy;
	
	private Map<String, Object> otherFields = new TreeMap<>();
	
	public String getType() {
		return type;
	}
	
	public String getOperationName() {
		return operation;
	}
	
	public String getSignedBy() {
		return signedBy;
	}
	
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public void setSignedBy(String signedBy) {
		this.signedBy = signedBy;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, String> getStringMap(String field) {
		return (Map<String, String>) otherFields.get(field);
	}
	
	public Number getNumberValue(String field) {
		return (Number) otherFields.get(field);
	}
	
	public String getStringValue(String field) {
		return (String) otherFields.get(field);
	}
	
	public void putStringValue(String key, String value) {
		otherFields.put(key, value);
	}
	
	public void putObjectValue(String key, Object value) {
		otherFields.put(key, value);
	}
	
	public Object remove(String key) {
		return otherFields.remove(key);
	}
	
	
	public static class OpDefinitionBeanAdapter implements JsonDeserializer<OpDefinitionBean>,
			JsonSerializer<OpDefinitionBean> {
		private static final String F_OPERATION = "operation";
		private static final String F_TYPE = "type";
		
		@Override
		public OpDefinitionBean deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			JsonObject o = json.getAsJsonObject();
			OpDefinitionBean bn = new OpDefinitionBean();
			bn.operation = o.get(F_OPERATION).getAsString();
			bn.type = o.get(F_TYPE).getAsString();
			if(o.has(F_SIGNED_BY)) {
				bn.signedBy = o.get(F_SIGNED_BY).getAsString();
			}
			bn.otherFields = context.deserialize(o, Map.class); 
			return bn;
		}

		@Override
		public JsonElement serialize(OpDefinitionBean src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject o = new JsonObject();
			o.addProperty(F_OPERATION, src.operation);
			o.addProperty(F_TYPE, src.type);
			o.addProperty(F_SIGNED_BY, src.signedBy);
			for(String k : src.otherFields.keySet()) {
				Object ob = src.otherFields.get(k);
				o.add(k, context.serialize(ob));
			}
			return o;
		}

	}
	
}
