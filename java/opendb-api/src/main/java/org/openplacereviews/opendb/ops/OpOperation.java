package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.util.ArrayList;
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
	
	public static final String F_REF = "ref";
	public static final String F_NEW = "new";
	public static final String F_OLD = "old";
	
	public static final String F_NAME = "name";
	public static final String F_COMMENT = "comment";
	
	private String type;
	private List<OpObject> newObjects = new LinkedList<OpObject>();
	
	public OpOperation() {
		this.operation = null;
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
	
	public String getType() {
		return type;
	}
	
	public String getHash() {
		return getStringValue(F_HASH);
	}
	
	public String getRawHash() {
		String rw = getStringValue(F_HASH);
		// drop algorithm and everything else
		if(rw != null) {
			rw = rw.substring(rw.lastIndexOf(':') + 1);
		}
		return rw;
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
	
	public void addNew(OpObject o) {
		newObjects.add(o);
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
	
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((newObjects == null) ? 0 : newObjects.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		OpOperation other = (OpOperation) obj;
		if (newObjects == null) {
			if (other.newObjects != null)
				return false;
		} else if (!newObjects.equals(other.newObjects))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}



	public static class OpOperationBeanAdapter implements JsonDeserializer<OpOperation>,
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
			TreeMap<String, Object> tm = new TreeMap<>(src.fields);
			tm.put(F_TYPE, src.type);
			if(src.hasNew()) {
				List<Map<String, Object>> list = new ArrayList<>();
				for (OpObject obj : src.newObjects) {
					list.add(obj.fields);
				}
				tm.put(F_NEW, list);
			}
			JsonObject o = (JsonObject) context.serialize(tm);
			return o;
		}

	}
}
