package org.openplacereviews.opendb.ops;

import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.DBConsensusManager.DBStaleException;
import org.openplacereviews.opendb.util.OUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;


class OpPrivateObjectInstancesById {

	private final String type;
	private Map<CompoundKey, OpObject> objects = new ConcurrentHashMap<>();
	private volatile CacheObject cacheObject;
	private Map<Object, CacheObject> cacheMap = null;
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

	/**
	 * returns OpObject.NULL if deleted
	 */
	public OpObject getObjectById(List<String> key) throws DBStaleException {
		CompoundKey k = new CompoundKey(0, key);
		return getByKey(k);
	}
	

	Map<CompoundKey, OpObject> getRawObjects() {
		if (dbAccess != null) {
			throw new UnsupportedOperationException();
		}
		return objects;
	}

	@SuppressWarnings("unchecked")
	public Stream<Entry<CompoundKey, OpObject>> fetchObjects(ObjectsSearchRequest request, 
			OpIndexColumn col, Object... args) throws DBStaleException {
		// limit will be negative
		int limit = request.limit - request.result.size();
		Stream<Entry<CompoundKey, OpObject>> stream;
		if (dbAccess != null) {
			if (col == null) {
				stream = dbAccess.streamObjects(type, limit);
			} else {
				stream = dbAccess.streamObjects(type, limit, col.getDbCondition(request, args));
			}
		} else {
			stream = objects.entrySet().stream();
			if(col != null){
				stream = stream.filter(new Predicate<Entry<CompoundKey, OpObject>>() {
					@Override
					public boolean test(Entry<CompoundKey, OpObject> t) {
						return col.accept(t.getValue(), request, args);
					}
				});
			}
		}
		if (request.internalMapToFilterDuplicates == null) {
			request.internalMapToFilterDuplicates = new HashSet<CompoundKey>();
		}
		final Set<CompoundKey> mp = (Set<CompoundKey>) request.internalMapToFilterDuplicates;
		return stream.filter(new Predicate<Entry<CompoundKey, OpObject>>() {

			@Override
			public boolean test(Entry<CompoundKey, OpObject> entr) {
				if (!mp.contains(entr.getKey())) {
					mp.add(entr.getKey());
				}
				return entr.getValue() != OpObject.NULL && entr.getValue() != null;
			}
		});
	}

	OpObject getByKey(CompoundKey k) throws DBStaleException {
		OpObject obj ;
		if (dbAccess != null) {
			obj = dbAccess.getObjectById(type, k);
		} else {
			obj = objects.get(k);
		}
		return obj;
	}

	public OpObject getObjectById(String primaryKey, String secondaryKey) throws DBStaleException {
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
		cacheMap = null;
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
	
	public CacheObject getIndexCacheObject(Object index) {
		Map<Object, CacheObject> mp = cacheMap;
		if (mp != null) {
			CacheObject co = mp.get(index);
			if (co != null && co.cacheVersion == editVersion.intValue()) {
				return co;
			}
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
	
	void setCacheIndexObject(Object index, Object cacheObject, int cacheVersion) {
		if (cacheVersion == editVersion.intValue()) {
			Map<Object, CacheObject> mp = cacheMap;
			if(mp == null) {
				mp = new ConcurrentHashMap<>();
				this.cacheMap = mp;
			}
			mp.put(index, new CacheObject(cacheObject, cacheVersion));
		}
	}

	public String getType() {
		return type;
	}

}
