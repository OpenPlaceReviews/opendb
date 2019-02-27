package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.openplacereviews.opendb.OUtils;


public class ObjectInstancesById {

	private final String type;
	private ObjectInstancesById parentInfo;
	private Map<ListKey, OpObject> objects = new ConcurrentHashMap<>();
	
	public ObjectInstancesById(String type, ObjectInstancesById pi) {
		this.type = type;
		this.parentInfo = pi;
	}
	
	public OpObject getObjectById(int subind, List<String> key) {
		ListKey k = new ListKey(subind, key);
		return getByKey(k);
	}

	private OpObject getByKey(ListKey k) {
		OpObject o = objects.get(k);
		ObjectInstancesById p = parentInfo;
		while(o == null && p != null) {
			o = p.objects.get(k);
			p = p.parentInfo;
		}
		return o;
	}
	
	public OpObject getObjectById(String primaryKey, String secondaryKey) {
		return getByKey(new ListKey(primaryKey, secondaryKey)); 
	}
	
	public void add(List<String> id, OpObject newObj) {
		if(newObj == null) {
			objects.put(new ListKey(0, id), newObj);
		} else {
			objects.remove(new ListKey(0, id));
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
