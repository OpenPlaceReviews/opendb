package org.opengeoreviews.opendb.ops;

import java.util.Map;
import java.util.TreeMap;

public class OpDefinitionBean {

	private String type;
	
	private String name;
	
	private Map<String, Object> otherFields = new TreeMap<>();
	
	public String getType() {
		return type;
	}
	
	public String getName() {
		return name;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, String> getStringMap(String field) {
		return (Map<String, String>) otherFields.get(field);
	}
	
	public String getStringValue(String field) {
		return (String) otherFields.get(field);
	}
	
}
