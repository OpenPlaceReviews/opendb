package org.openplacereviews.opendb.ops;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.*;

public class OpOperation extends OpObject {

	public static final String F_TYPE = "type";
	public static final String F_SIGNED_BY = "signed_by";
	public static final String F_HASH = "hash";

	public static final String F_SIGNATURE = "signature";

	public static final String F_REF = "ref";
	public static final String F_CREATE = "create";
	public static final String F_EDIT = "edit";
	public static final String F_DELETE = "delete";
	public static final String F_NEW = "new";
	public static final String F_OLD = "old";

	public static final String F_NAME = "name";
	public static final String F_COMMENT = "comment";
	protected String type;
	private List<OpObject> createdObjects = new LinkedList<OpObject>();
	private List<OpObject> editedOldObjects = new LinkedList<OpObject>();
	private List<OpObject> editedNewObjects = new LinkedList<OpObject>();

	public OpOperation() {
	}

	public OpOperation(OpOperation cp, boolean copyCacheFields) {
		super(cp, copyCacheFields);
		this.type = cp.type;
		for (OpObject o : cp.createdObjects) {
			this.createdObjects.add(new OpObject(o, copyCacheFields));
		}
		for (OpObject o : cp.editedOldObjects) {
			this.editedOldObjects.add(new OpObject(o, copyCacheFields));
		}
		for (OpObject o : cp.editedNewObjects) {
			this.editedNewObjects.add(new OpObject(o, copyCacheFields));
		}

	}

	public String getOperationType() {
		return type;
	}

	protected void updateObjectsRef() {
		for (OpObject o : createdObjects) {
			o.setParentOp(this);
		}
	}

	public String getType() {
		return type;
	}

	public void setType(String name) {
		checkNotImmutable();
		type = name;
		updateObjectsRef();
	}

	public OpOperation makeImmutable() {
		isImmutable = true;
		for (OpObject o : createdObjects) {
			o.makeImmutable();
		}
		return this;
	}

	public void addOtherSignedBy(String value) {
		super.addOrSetStringValue(F_SIGNED_BY, value);
	}

	public List<String> getSignedBy() {
		return getStringList(F_SIGNED_BY);
	}

	public void setSignedBy(String value) {
		putStringValue(F_SIGNED_BY, value);
	}

	public String getHash() {
		return getStringValue(F_HASH);
	}

	public String getRawHash() {
		String rw = getStringValue(F_HASH);
		// drop algorithm and everything else
		if (rw != null) {
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

	@SuppressWarnings("unchecked")
	public List<List<String>> getDeleted() {
		List<List<String>> l = (List<List<String>>) fields.get(F_DELETE);
		if (l == null) {
			return Collections.emptyList();
		}
		return l;
	}

	public void addDeleted(List<String> id) {
		if (!fields.containsKey(F_DELETE)) {
			ArrayList<List<String>> lst = new ArrayList<>();
			lst.add(id);
			putObjectValue(F_DELETE, lst);
		} else {
			getDeleted().add(id);
		}
	}

	public List<OpObject> getCreated() {
		return createdObjects;
	}

	public void addCreated(OpObject o) {
		checkNotImmutable();
		createdObjects.add(o);
		if (type != null) {
			o.setParentOp(this);
		}
	}

	public void addEdited(OpObject newO, OpObject oldO) {
		checkNotImmutable();
		editedNewObjects.add(newO);
		editedOldObjects.add(oldO);
		if (type != null) {
			newO.setParentOp(this);
		}
	}

	public List<OpObject> getEditedNew() {
		return editedNewObjects;
	}

	public List<OpObject> getEditedOld() {
		return editedOldObjects;
	}

	public boolean hasCreated() {
		return createdObjects.size() > 0;
	}

	public boolean hasEdited() {
		return editedNewObjects.size() > 0;
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
		result = prime * result + ((createdObjects == null) ? 0 : createdObjects.hashCode());
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
		if (createdObjects == null) {
			if (other.createdObjects != null)
				return false;
		} else
			if (!createdObjects.equals(other.createdObjects))
				return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else
			if (!type.equals(other.type))
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
			if (tp != null) {
				String opType = tp.getAsString();
				op.type = opType;
			} else {
				op.type = "";
			}
			JsonElement createdObjs = jsonObj.remove(F_CREATE);
			if (createdObjs != null) {
				JsonArray ar = createdObjs.getAsJsonArray();
				for (int i = 0; i < ar.size(); i++) {
					op.addCreated(context.deserialize(ar.get(i), OpObject.class));
				}
			}

			JsonElement editedObjs = jsonObj.remove(F_EDIT);
			if (editedObjs != null) {
				for (JsonElement editElem : editedObjs.getAsJsonArray()) {
					OpObject newEditObj =
							context.deserialize(editElem.getAsJsonObject().getAsJsonObject(F_NEW), OpObject.class);
					OpObject deletedEditObj =
							context.deserialize(editElem.getAsJsonObject().getAsJsonObject(F_OLD), OpObject.class);
					op.addEdited(newEditObj, deletedEditObj);
				}
			}

			jsonObj.remove(F_EVAL);
			op.fields = context.deserialize(jsonObj, TreeMap.class);
			return op;
		}

		@Override
		public JsonElement serialize(OpOperation src, Type typeOfSrc, JsonSerializationContext context) {
			TreeMap<String, Object> tm = new TreeMap<>(fullOutput ? src.getMixedFieldsAndCacheMap() : src.fields);
			if (excludeHashAndSignature) {
				tm.remove(F_SIGNATURE);
				tm.remove(F_HASH);
			}
			tm.put(F_TYPE, src.type);
			if (src.hasCreated()) {
				tm.put(F_CREATE, context.serialize(src.createdObjects));
			}

			if (src.hasEdited()) {
				List<OpObject> newObject = src.getEditedNew();
				List<OpObject> oldObject = src.getEditedOld();
				List<Map<String, OpObject>> editedMap = new ArrayList<>(newObject.size());
				for (int i = 0; i < newObject.size(); i++) {
					Map<String, OpObject> editedObj = new TreeMap<>();
					editedObj.put(F_NEW, newObject.get(i));
					editedObj.put(F_OLD, oldObject.get(i));
					editedMap.add(editedObj);
				}
				tm.put(F_EDIT, context.serialize(editedMap));
			}

			return context.serialize(tm);
		}


	}
}
