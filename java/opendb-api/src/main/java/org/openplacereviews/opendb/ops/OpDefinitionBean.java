package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OpDefinitionBean  {
	
	public static final String F_HASH = "hash";
	public static final String F_OPERATION = "operation";
	public static final String F_OPERATION_NAME = "operation_name";
	public static final String F_OPERATION_TYPE = "operation_type";
	
	public static final String F_NAME = "name";
	public static final String F_SIGNATURE = "signature";
	public static final String F_SIGNATURE_HASH = "signature_hash";
	public static final String F_SIGNED_BY = "signed_by";
	public static final String SYSTEM_TYPE = "system";
	public static final String F_DEPENDENCIES = "dependencies";
	
	private String type;
	private String operation;
	private String signedBy;
	private List<String> otherSignedBy;
	private List<String> transientTxDependencies;
	
	private Map<String, Object> otherFields = new TreeMap<>();
	
	public OpDefinitionBean() {
	}
	
	public OpDefinitionBean(OpDefinitionBean cp) {
		this.type = cp.type;
		this.operation = cp.operation;
		this.signedBy = cp.signedBy;
		if(cp.otherSignedBy != null) {
			this.otherSignedBy = new ArrayList<String>(cp.otherSignedBy);
		}
		this.otherFields.putAll(cp.otherFields);
	}
	
	public String getOperationType() {
		return type;
	}
	
	public void setTransientTxDependencies(List<String> transientTxDependencies) {
		this.transientTxDependencies = transientTxDependencies;
	}
	
	public List<String> getTransientTxDependencies() {
		return transientTxDependencies;
	}
	
	public String getOperationName() {
		return operation;
	}
	
	public String getOperationId() {
		return type + ":" + operation;
	}
	
	public String getSignedBy() {
		return signedBy;
	}
	
	public List<String> getOtherSignedBy() {
		return otherSignedBy;
	}
	
	public String getHash() {
		return getStringValue(F_HASH);
	}
	
	public String getName() {
		return getStringValue(F_NAME);
	}
	
	public String getSignatureHash() {
		return getStringValue(F_SIGNATURE_HASH);
	}
	
	public Map<String, Object> getRawOtherFields() {
		return otherFields;
	}
	
	public boolean hasOneSignature() {
		return otherSignedBy == null || otherSignedBy.isEmpty();
	}
	
	public void addOtherSignedBy(String signedBy) {
		if(otherSignedBy == null) {
			otherSignedBy = new ArrayList<String>();
		}
		otherSignedBy.add(signedBy);
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
	
	@SuppressWarnings("unchecked")
	public List<String> getStringList(String field) {
		return (List<String>) otherFields.get(field);
	}
	
	@SuppressWarnings("unchecked")
	public List<Map<String, String>> getListStringMap(String field) {
		return (List<Map<String, String>>) otherFields.get(field);
	}
	
	public Number getNumberValue(String field) {
		return (Number) otherFields.get(field);
	}
	
	public String getStringValue(String field) {
		return (String) otherFields.get(field);
	}
	
	public void putStringValue(String key, String value) {
		if(value == null) {
			otherFields.remove(key);
		} else {
			otherFields.put(key, value);
		}
	}
	
	public void putObjectValue(String key, Object value) {
		if(value == null) {
			otherFields.remove(key);
		} else {
			otherFields.put(key, value);
		}
	}
	
	public Object remove(String key) {
		return otherFields.remove(key);
	}
	
	public static class OpDefinitionBeanAdapter implements JsonDeserializer<OpDefinitionBean>,
			JsonSerializer<OpDefinitionBean> {
		private static final String F_OPERATION = "operation";
		
		@Override
		public OpDefinitionBean deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			JsonObject o = json.getAsJsonObject();
			OpDefinitionBean bn = new OpDefinitionBean();
			String op = o.get(F_OPERATION).getAsString();
			int s = op.indexOf(':');
			if(s == -1) {
				bn.type = SYSTEM_TYPE;
				bn.operation = op;
			} else {
				bn.type = op.substring(0, s);
				bn.operation = op.substring(s+1);
			}
			if(o.has(F_SIGNED_BY)) {
				if(o.get(F_SIGNED_BY).isJsonArray()) {
					bn.otherSignedBy = new ArrayList<String>();
					JsonArray array = o.get(F_SIGNED_BY).getAsJsonArray();
					for (int i = 0; i < array.size(); i++) {
						String signedBy = array.get(i).getAsString();
						if (i == 0) {
							bn.signedBy = signedBy;
						} else {
							bn.otherSignedBy.add(signedBy);
						}
					}
				} else {
					bn.signedBy = o.get(F_SIGNED_BY).getAsString();
				}
			}
			bn.otherFields = context.deserialize(o, TreeMap.class); 
			bn.otherFields.remove(F_OPERATION);
			bn.otherFields.remove(F_SIGNED_BY);
			
			return bn;
		}

		@Override
		public JsonElement serialize(OpDefinitionBean src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject o = new JsonObject();
			o.addProperty(F_OPERATION, src.type + ":" + src.operation);
			if(src.otherSignedBy == null || src.otherSignedBy.size() == 0) {
				o.addProperty(F_SIGNED_BY, src.signedBy);
			} else {
				JsonArray arr = new JsonArray();
				arr.add(src.signedBy);
				for (String s : src.otherSignedBy) {
					arr.add(s);
				}
				o.add(F_SIGNED_BY, arr);
			}
			for(String k : src.otherFields.keySet()) {
				Object ob = src.otherFields.get(k);
				o.add(k, context.serialize(ob));
			}
			return o;
		}

	}
	
}
