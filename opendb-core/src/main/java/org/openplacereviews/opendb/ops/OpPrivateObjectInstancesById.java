package org.openplacereviews.opendb.ops;

import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.util.OUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


class OpPrivateObjectInstancesById {

	private final String type;
	private Map<CompoundKey, OpObject> objects = new ConcurrentHashMap<>();
	private volatile CacheObject cacheObject;
	private AtomicInteger editVersion = new AtomicInteger(0);
	private final BlockDbAccessInterface dbAccess;

	static class CacheObject {
		Object cacheObject;
		int cacheVersion;

		public CacheObject(Object cacheObject, int cacheVersion) {
			this.cacheObject = cacheObject;
			this.cacheVersion = cacheVersion;
		}
	}

	public OpPrivateObjectInstancesById(String type, BlockDbAccessInterface dbAccess) {
		this.type = type;
		this.dbAccess = dbAccess;
	}

	public OpObject getObjectById(List<String> key) {
		CompoundKey k = new CompoundKey(0, key);
		return getByKey(k);
	}

	public Map<CompoundKey, OpObject> getAllObjects() {
		if (dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		if (objects.containsValue(OpObject.NULL)) {
			LinkedHashMap<CompoundKey, OpObject> res = new LinkedHashMap<>();
			Iterator<Entry<CompoundKey, OpObject>> it = objects.entrySet().iterator();
			while (it.hasNext()) {
				Entry<CompoundKey, OpObject> e = it.next();
				res.put(e.getKey(), e.getValue() == OpObject.NULL ? null : e.getValue());
			}
			return res;
		}
		return objects;
	}

	@SuppressWarnings("unchecked")
	public void fetchAllObjects(ObjectsSearchRequest request) {

		Map<CompoundKey, OpObject> allObjects = objects;
		if (dbAccess != null) {
			allObjects = dbAccess.getAllObjects(type, request);
		}
		if(request.internalMapToFilterDuplicates == null) {
			request.internalMapToFilterDuplicates = new HashMap<CompoundKey, OpObject>();
			Map<CompoundKey, OpObject> mp = (Map<CompoundKey, OpObject>) request.internalMapToFilterDuplicates;
			for (OpObject o : objects.values()) {
				if (o != OpObject.NULL) {
					request.result.add(o);
				}
				mp.put(new CompoundKey(0, o.getId()), o);
			}
		} else {
			Map<CompoundKey, OpObject> mp = (Map<CompoundKey, OpObject>) request.internalMapToFilterDuplicates;
			Iterator<Entry<CompoundKey, OpObject>> it = allObjects.entrySet().iterator();
			while (it.hasNext()) {
				Entry<CompoundKey, OpObject> k = it.next();
				if (!mp.containsKey(k.getKey())) {
					if (k.getValue() != OpObject.NULL) {
						request.result.add(k.getValue());
					}
					mp.put(k.getKey(), k.getValue());
				}
			}
		}
	}

	public void fetchAllObjectHeaders(ObjectsSearchRequest request) {

		Map<CompoundKey, OpObject> allObjects = objects;
		if (dbAccess != null) {
			allObjects = dbAccess.getAllObjects(type, request);
		}
		if(request.internalMapToFilterDuplicates == null) {
			request.internalMapToFilterDuplicates = new HashMap<CompoundKey, OpObject>();
			Map<CompoundKey, List<String>> mp = (Map<CompoundKey, List<String>>) request.internalMapToFilterDuplicates;
			for (OpObject o : objects.values()) {
				if (o != OpObject.NULL) {
					request.resultWithHeaders.add(o.getStringList(request.field));
				}
				mp.put(new CompoundKey(0, o.getId()), o.getId());
			}
		} else {
			Map<CompoundKey, OpObject> mp = (Map<CompoundKey, OpObject>) request.internalMapToFilterDuplicates;
			Iterator<Entry<CompoundKey, OpObject>> it = allObjects.entrySet().iterator();
			while (it.hasNext()) {
				Entry<CompoundKey, OpObject> k = it.next();
				if (!mp.containsKey(k.getValue().getStringList(request.field))) {
					if (k.getValue() != OpObject.NULL) {
						request.resultWithHeaders.add(k.getValue().getStringList(request.field));
					}
					mp.put(k.getKey(), k.getValue());
				}
			}
		}
	}

	private OpObject getByKey(CompoundKey k) {
		if (dbAccess != null) {
			return dbAccess.getObjectById(type, k);
		}
		OpObject obj = objects.get(k);
		if (obj == OpObject.NULL) {
			return null;
		}
		return obj;
	}

	public OpObject getObjectById(String primaryKey, String secondaryKey) {
		return getByKey(new CompoundKey(primaryKey, secondaryKey));
	}

	void putObjects(OpPrivateObjectInstancesById prev, boolean overwrite) {
		if (dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		if (prev == null) {
			return;
		}
		if (!OUtils.equals(prev.type, type)) {
			throw new IllegalStateException(
					String.format("Previous type %s doesn't match current type %s", prev.type, type));
		}
		Iterator<Entry<CompoundKey, OpObject>> objs = prev.objects.entrySet().iterator();
		while (objs.hasNext()) {
			Entry<CompoundKey, OpObject> e = objs.next();
			if (!objects.containsKey(e.getKey()) || overwrite) {
				objects.put(e.getKey(), e.getValue());
			}
		}
		resetAfterEdit();

	}

	void resetAfterEdit() {
		editVersion.incrementAndGet();
		cacheObject = null;
	}

	public void add(List<String> id, OpObject newObj) {
		if (dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		objects.put(new CompoundKey(0, id), newObj == null ? OpObject.NULL : newObj);
		resetAfterEdit();
	}

	public CacheObject getCacheObject() {
		CacheObject c = cacheObject;
		if (c != null && c.cacheVersion == editVersion.intValue()) {
			return c;
		}
		return null;
	}

	public int getEditVersion() {
		return editVersion.intValue();
	}

	void setCacheObject(Object cacheObject, int cacheVersion) {
		if (cacheVersion == editVersion.intValue()) {
			this.cacheObject = new CacheObject(cacheObject, cacheVersion);
		}
	}

	public String getType() {
		return type;
	}

}
