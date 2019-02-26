package org.openplacereviews.opendb.ops;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.openplacereviews.opendb.OUtils;


public class ObjectInfo {

	private final OpBlockChain blc;
	private final String type;
//	private final ObjectInfo parentInfo;
	private Map<ListKey, OpObject> objects = new ConcurrentHashMap<>();
	
	
	public ObjectInfo(String type, OpBlockChain b, ObjectInfo pi) {
		this.type = type;
		this.blc = b;
//		this.parentInfo = pi;
		if(pi != null) {
			this.objects.putAll(pi.objects);
		}
	}
	
	public OpObject getObjectByFullName(List<String> o) {
		return objects.get(new ListKey(o));
	}	
	
	public void add(List<String> id, OpObject newObj) {
		objects.put(new ListKey(id), newObj);
	}
	
	
	public Collection<OpObject> getAllObjects() {
		return objects.values();
	}
	
	public String getType() {
		return type;
	}
	
	public OpBlockChain getBlc() {
		return blc;
	}


	private static class ListKey {
		final List<String> list;
		private ListKey(List<String> l) {
			this.list = l;
		}
		@Override
		public int hashCode() {
	        int result = 1;
	        for (Object element : list) {
	            result = 31 * result + (element == null ? 0 : element.hashCode());
	        }
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof ListKey)) {
				return false;
			}
			ListKey l2 = (ListKey) obj;
			if(l2.list.size() != list.size()) {
				return false;
			}
			for(int i = 0; i < list.size(); i++) {
				if(!OUtils.equals(l2.list.get(i), list.get(i))) {
					return false;
				}
			}
			return true;
		}
	}


	
}
