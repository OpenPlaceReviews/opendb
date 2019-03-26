package org.openplacereviews.opendb.ops;

import java.lang.reflect.Type;
import java.util.LinkedList;
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
	
	private List<OpObject> newObjects = new LinkedList<OpObject>();
	protected String type;
	
	public OpOperation() {
	}
	
	public OpOperation(OpOperation cp, boolean copyCacheFields) {
		super(cp, copyCacheFields);
		this.type = cp.type;
		for(OpObject o : cp.newObjects) {
			this.newObjects.add(new OpObject(o, copyCacheFields));
		}
	}
	
	public String getOperationType() {
		return type;
	}
	
	public void setType(String name) {
		checkNotImmutable();
		type = name;
		updateObjectsRef();
	}

	protected void updateObjectsRef() {
		for(OpObject o : newObjects) {
			o.setParentOp(this);
		}
	}
	
	public String getType() {
		return type;
	}
	
	public OpOperation makeImmutable() {
		isImmutable = true;
		for(OpObject o : newObjects) {
			o.makeImmutable();
		}
		return this;
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
		checkNotImmutable();
		newObjects.add(o);
		if(type != null) {
			o.setParentOp(this);
		}
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

		// plain serialization to calculate hash
		private boolean excludeHashAndSignature;
		private boolean fullOutput;

		public OpOperationBeanAdapter(boolean fullOutput, boolean excludeHashAndSignature) {
			this.excludeHashAndSignature = excludeHashAndSignature;
			this.fullOutput = fullOutput;
		}
		
		public OpOperationBeanAdapter(boolean fullOutput) {
			this.fullOutput = fullOutput;
			this.excludeHashAndSignature = false;
		}
		
		
		@Override
		public OpOperation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {
			JsonObject jsonObj = json.getAsJsonObject();
			OpOperation op = new OpOperation();
			JsonElement tp = jsonObj.remove(F_TYPE);
			if(tp != null) {
				String opType = tp.getAsString();
				op.type = opType;
			} else {
				op.type = "";
			}
			JsonElement newObjs = jsonObj.remove(F_NEW);
			if(newObjs != null) {
				JsonArray ar = newObjs.getAsJsonArray();
				for(int i = 0; i < ar.size(); i++) {
					op.addNew(context.deserialize(ar.get(i), OpObject.class));
				}
			}
			jsonObj.remove(F_EVAL);
			op.fields = context.deserialize(jsonObj, TreeMap.class);
			return op;
		}

		@Override
		public JsonElement serialize(OpOperation src, Type typeOfSrc, JsonSerializationContext context) {
			TreeMap<String, Object> tm = new TreeMap<>(fullOutput ? src.getMixedFieldsAndCacheMap() : src.fields);
			if(excludeHashAndSignature) {
				tm.remove(F_SIGNATURE);
				tm.remove(F_HASH);
			}
			tm.put(F_TYPE, src.type);
			if(src.hasNew()) {
				tm.put(F_NEW, context.serialize(src.newObjects));
			}
			return context.serialize(tm);
		}

	}
}
