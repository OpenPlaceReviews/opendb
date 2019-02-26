package org.openplacereviews.opendb.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.TreeMap;

import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

@Component
public class JsonFormatter {

	private Gson gson;

	public JsonFormatter() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpOperation.class, new OpOperation.OpDefinitionBeanAdapter());
		gson = builder.create();
	}
	
//	operations to parse / format related
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/opr-"+id+".json");
    }
	
	public OpBlock parseBootstrapBlock(String id) {
		return gson.fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
	}
	
	public OpOperation parseOperation(String opJson) {
		return gson.fromJson(opJson, OpOperation.class);
	}
	
	public OpBlock parseBlock(String opJson) {
		return gson.fromJson(opJson, OpBlock.class);
	}
	
	public String toJson(OpBlock bl) {
		return gson.toJson(bl);
	}
	
	public JsonObject toJsonObject(OpBlock bl) {
		return gson.toJsonTree(bl).getAsJsonObject();
	}
	
	public JsonObject toJsonObject(OpOperation o) {
		return gson.toJsonTree(o).getAsJsonObject();
	}
	
	public String objectToJson(Object o) {
		return gson.toJson(o);
	}
	
	public String toJson(OpOperation op) {
		return gson.toJson(op);
	}

	@SuppressWarnings("unchecked")
	public TreeMap<String, Object> fromJsonToTreeMap(String json) {
		return gson.fromJson(json, TreeMap.class);
	}
	
	public JsonObject fromJsonToJsonObject(String json) {
		return gson.fromJson(json, JsonObject.class);
	}
	
	
}
