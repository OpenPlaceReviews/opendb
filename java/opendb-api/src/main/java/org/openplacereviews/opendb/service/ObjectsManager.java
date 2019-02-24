package org.openplacereviews.opendb.service;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.openplacereviews.opendb.ops.OpObject;
import org.springframework.stereotype.Service;

@Service
public class ObjectsManager {

	public ObjectsTreeStructure root = new ObjectsTreeStructure("");
	
	public static class ObjectsTreeStructure {
		String name;
		
		Map<String, ObjectsTreeStructure> subTrees = new TreeMap<String, ObjectsTreeStructure>();
		Map<String, OpObject> objects = new HashMap<String, OpObject>();
		
		private ObjectsTreeStructure(String name) {
			this.name = name;
		}
	}
	
}
