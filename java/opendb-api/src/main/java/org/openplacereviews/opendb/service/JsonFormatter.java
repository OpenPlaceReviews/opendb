package org.openplacereviews.opendb.service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.TreeMap;

import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Component
public class JsonFormatter {

	private Gson gson;

	public JsonFormatter() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpDefinitionBean.class, new OpDefinitionBean.OpDefinitionBeanAdapter());
		gson = builder.create();
	}
	
//	operations to parse / format related
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }
	
	public OpBlock parseBootstrapBlock(String id) {
		return gson.fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
	}
	
	public OpDefinitionBean parseOperation(String opJson) {
		return gson.fromJson(opJson, OpDefinitionBean.class);
	}
	public String toJson(OpBlock bl) {
		return gson.toJson(bl);
	}
	
	public String toJson(OpDefinitionBean op) {
		return gson.toJson(op);
	}

	@SuppressWarnings("unchecked")
	public TreeMap<String, Object> fromJsonToTreeMap(String json) {
		return gson.fromJson(json, TreeMap.class);
	}
	
	
}
