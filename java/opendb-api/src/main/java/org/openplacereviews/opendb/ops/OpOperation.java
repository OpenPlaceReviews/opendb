package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OpOperation extends OpObject {
	
	public static final String F_TYPE = "type";
	public static final String F_SIGNED_BY = "signed_by";
	public static final String F_HASH = "hash";
	
	public static final String F_SIGNATURE = "signature";
	// transient info about validation timing etc
	public static final String F_VALIDATION = "validation";
	
	public static final String F_REF = "ref";
	public static final String F_NEW = "new";
	public static final String F_OLD = "old";
	
	public static final String F_NAME = "name";
	public static final String F_COMMENT = "comment";
	
	private String type;
	private List<OpObject> newObjects = new LinkedList<OpObject>();
	
	public OpOperation() {
		this.operation = this;
	}
	
	public OpOperation(OpOperation cp) {
		super(cp);
		this.type = cp.type;
		this.operation = this;
	}
	
	public String getOperationType() {
		return type;
	}
	
	public void setOperationType(String name) {
		type = name;
	}
	
	public void setSignedBy(String value) {
		putStringValue(F_SIGNED_BY, value);
	}
	
	public void addOtherSignedBy(String value) {
		super.addOrSetStringValue(F_SIGNED_BY, value);
	}
	
	public List<String> getSignedBy() {
		return getStringList(F_SIGNED_BY);
	}
	
	
	
	@Override
	public String getType() {
		return TYPE_OP;
	}
	
	public String getHash() {
		return getStringValue(F_HASH);
	}
	
	public List<String> getSignatureList() {
		return getStringList(F_SIGNATURE);
	}
	
	public Map<String, List<String>> getRef() {
		return getMapStringList(F_REF);
	}
	
	public List<String> getOld() {
		return getStringList(F_OLD);
	}
	
	public List<OpObject> getNew() {
		return newObjects;
	}
	
	public boolean hasNew() {
		return newObjects.size() > 0;
	}
	
	public String getName() {
		return getStringValue(F_NAME);
	}
	
	public String getComment() {
		return getStringValue(F_COMMENT);
	}
	
	public void clearNonSignificantBlockFields() {
		String hash = getHash();
		String name = getName();
		String cmt = getComment();
		
		fields.clear();

		putStringValue(F_HASH, hash);
		putStringValue(F_NAME, name);
		putStringValue(F_COMMENT, cmt);
	}
	
	public static class OpDefinitionBeanAdapter implements JsonDeserializer<OpOperation>,
			JsonSerializer<OpOperation> {
		
		@Override
		public OpOperation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			JsonObject jsonObj = json.getAsJsonObject();
			OpOperation op = new OpOperation();
			String opType = jsonObj.get(F_TYPE).getAsString();
			op.type = opType;
			op.fields = context.deserialize(jsonObj, TreeMap.class);
			op.fields.remove(F_TYPE);
			List<Map<String, Object>> lst = op.getListStringObjMap(F_NEW);
			op.fields.remove(F_NEW);
			if(lst != null) {
				for(Map<String, Object> mp : lst) {
					OpObject e = new OpObject(op, mp);
					op.newObjects.add(e);
				}
			}
			return op;
		}

		@Override
		public JsonElement serialize(OpOperation src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject o = new JsonObject();
			o.addProperty(F_TYPE, src.type);
			TreeMap<String, Object> tm = new TreeMap<>(src.fields);
			tm.put(F_TYPE, src.type);
			if(src.hasNew()) {
				tm.put(F_NEW, src.newObjects);
			}
			
			for(String k : src.fields.keySet()) {
				Object ob = src.fields.get(k);
				o.add(k, context.serialize(ob));
			}
			return o;
		}

	}


	
}
