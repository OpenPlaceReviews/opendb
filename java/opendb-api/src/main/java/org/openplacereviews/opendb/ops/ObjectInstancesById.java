package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;


public class ObjectInstancesById {

	private final String type;
	private Map<ListKey, OpObject> objects = new ConcurrentHashMap<>();
	private volatile CacheObject cacheObject;
	private AtomicInteger editVersion = new AtomicInteger(0);
	
	static class CacheObject {
		Object cacheObject;
		int cacheVersion;
		public CacheObject(Object cacheObject, int cacheVersion) {
			this.cacheObject = cacheObject;
			this.cacheVersion = cacheVersion;
		}
	}
	
	public ObjectInstancesById(String type) {
		this.type = type;
	}
	
	public OpObject getObjectById(List<String> key) {
		ListKey k = new ListKey(0, key);
		return getByKey(k);
	}
	
	@SuppressWarnings("unchecked")
	public void fetchAllObjects(ObjectsSearchRequest request) {
		if(request.result.isEmpty()) {
			request.result.addAll(objects.values());
			request.internalMapToFilterDuplicates = new HashMap<ListKey, OpObject>(objects); 
		} else {
			Map<ListKey, OpObject> mp = (Map<ListKey, OpObject>) request.internalMapToFilterDuplicates;
			Iterator<Entry<ListKey, OpObject>> it = objects.entrySet().iterator();
			while(it.hasNext()) {
				Entry<ListKey, OpObject> k = it.next();
				if(!mp.containsKey(k.getKey())) {
					request.result.add(k.getValue());
					mp.put(k.getKey(), k.getValue());
				}
			}
		}
	}
	
	private OpObject getByKey(ListKey k) {
		return objects.get(k);
	}
	
	public OpObject getObjectById(String primaryKey, String secondaryKey) {
		return getByKey(new ListKey(primaryKey, secondaryKey)); 
	}
	
	void putObjects(ObjectInstancesById prev, boolean overwrite) {
		if(prev == null) {
			return;
		}
		if(!OUtils.equals(prev.type, type)) {
			throw new IllegalStateException(String.format("Previous type %s doesn't match current type %s", prev.type, type)); 
		}
		Iterator<Entry<ListKey, OpObject>> objs = prev.objects.entrySet().iterator();
		while(objs.hasNext()) {
			Entry<ListKey, OpObject> e = objs.next();
			if(!objects.containsKey(e.getKey()) || overwrite) {
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
		if(newObj != null) {
			objects.put(new ListKey(0, id), newObj);
		} else {
			objects.remove(new ListKey(0, id));
		}
		resetAfterEdit();
	}
	
	public CacheObject getCacheObject() {
		CacheObject c = cacheObject;
		if(c.cacheVersion == editVersion.intValue()) {
			return cacheObject;
		}
		return null;
	}
	
	public int getEditVersion() {
		return editVersion.intValue();
	}
	
	void setCacheObject(Object cacheObject, int cacheVersion) {
		if(cacheVersion == editVersion.intValue()) {
			this.cacheObject = new CacheObject(cacheObject, cacheVersion);
		}
	}
	
	public String getType() {
		return type;
	}
	
	private static class ListKey {
		final String first;
		final String second;
		final int hashcode;
		final List<String> others ;
		
		
		private ListKey(String first, String second) {
			int result = 1;
			this.first = first;
			if(first != null) {
				result = 31 * result + (first == null ? 0 : first.hashCode());
			}
			this.second = second;
			if(second != null) {
				result = 31 * result + (second == null ? 0 : second.hashCode());
			}
			this.hashcode = result;
			this.others = null;
		}
		
		private ListKey(int subInd, List<String> l) {
			int result = 1;
			String first = null;
			String second = null;
			List<String> others = null;
			for(int i = subInd; i < l.size(); i++) {
				String element = l.get(i);
				if(element == null) {
					throw new IllegalArgumentException("Primary key coudln't be null: " + l);
				}
				if(i == subInd) {
					first = element;   
				} else if(i == subInd + 1) {
					second = element;
				} else if(i == subInd + 2) {
					others = new ArrayList<String>();
					others.add(element);
				} else {
					others.add(element);
				}
				result = 31 * result + (element == null ? 0 : element.hashCode());
	        }
			this.first = first;
			this.second = second;
			this.hashcode = result;
			this.others = others;
		}
		
		@Override
		public int hashCode() {
			return hashcode;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof ListKey)) {
				return false;
			}
			ListKey l2 = (ListKey) obj;
			if(!OUtils.equals(first, l2.first)) {
				return false;
			}
			if(!OUtils.equals(second, l2.second)) {
				return false;
			}
			if(!OUtils.equals(others, l2.others)) {
				return false;
			}
			return true;
		}
	}

}
