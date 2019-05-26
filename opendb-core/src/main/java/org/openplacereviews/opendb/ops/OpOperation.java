package org.openplacereviews.opendb.ops;

import com.google.gson.*;
import org.openplacereviews.opendb.service.ipfs.dto.ImageDTO;

import java.lang.reflect.Type;
import java.util.*;

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
	protected List<ImageDTO> images;
	
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
	
	public void addOld(String hash, int ind) {
		addOrSetStringValue(F_OLD, hash + ":" + ind);
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

	public List<ImageDTO> getImages() {
		return images;
	}

	public void setImages(List<ImageDTO> images) {
		this.images = images;
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
			Map treeMap = new Gson().fromJson(json, TreeMap.class);

			List<ImageDTO> imageDTOList = new ArrayList<>();
			getObject(treeMap, imageDTOList);

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
			op.images = imageDTOList;
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

		private void getObject(Map map, List<ImageDTO> array) {
			if (map.containsKey(F_TYPE) && map.get(F_TYPE).equals("#image")) {
				array.add(ImageDTO.of(map.get("hash").toString(), map.get("extension").toString(), map.get("cid").toString()));
			} else {
				map.keySet().forEach(key -> {
					if (map.get(key) instanceof Map) {
						getObject( (Map) map.get(key), array);
					}
					if (map.get(key) instanceof List) {
						if (!(((List) map.get(key)).isEmpty()) && !(((List) map.get(key)).get(0).getClass().equals(String.class))) {
							getObject( (List<Map>)map.get(key), array);
						}
					}
				});
			}
		}

		private void getObject(List<Map> list, List<ImageDTO> array) {
			for (Map map : list) {
				map.keySet().forEach(key -> {
					if (key.equals(F_TYPE) && map.get(key).equals("#image")) {
						array.add(ImageDTO.of(map.get("hash").toString(), map.get("extension").toString(), map.get("cid").toString()));
					} else {
						if (map.get(key) instanceof Map) {
							getObject( (Map) map.get(key), array);
						}
						if (map.get(key) instanceof List) {
							if (map.get(key).getClass().equals(Map.class)) {
								getObject((List<Map>) map.get(key), array);
							}
						}
					}
				});
			}
		}

	}
}
