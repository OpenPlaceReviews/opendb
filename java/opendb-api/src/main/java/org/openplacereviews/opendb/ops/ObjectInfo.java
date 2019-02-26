package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
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
	
	public OpObject getObjectById(int subind, List<String> o) {
		return objects.get(new ListKey(subind, o));
	}
	
	public OpObject getObjectById(String primaryKey, String secondaryKey) {
		return objects.get(new ListKey(primaryKey, secondaryKey));
	}
	
	public void add(List<String> id, OpObject newObj) {
		objects.put(new ListKey(0, id), newObj);
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
