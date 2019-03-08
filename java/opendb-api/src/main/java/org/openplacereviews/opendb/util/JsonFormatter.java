package org.openplacereviews.opendb.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;

@Component
public class JsonFormatter {

	private Gson gson;

	public JsonFormatter() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpOperation.class, new OpOperation.OpOperationBeanAdapter());
		builder.registerTypeAdapter(OpBlock.class, new OpBlock.OpBlockBeanAdapter());
		builder.registerTypeAdapter(TreeMap.class, new MapDeserializerDoubleAsIntFix());
		gson = builder.create();
	}
	
	public static class MapDeserializerDoubleAsIntFix implements JsonDeserializer<TreeMap<String, Object>>{

	    @Override  @SuppressWarnings("unchecked")
	    public TreeMap<String, Object> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
	        return (TreeMap<String, Object>) read(json);
	    }

	    public Object read(JsonElement in) {

	        if(in.isJsonArray()){
	            List<Object> list = new ArrayList<Object>();
	            JsonArray arr = in.getAsJsonArray();
	            for (JsonElement anArr : arr) {
	                list.add(read(anArr));
	            }
	            return list;
	        }else if(in.isJsonObject()){
	            Map<String, Object> map = new TreeMap<String, Object>();
	            JsonObject obj = in.getAsJsonObject();
	            Set<Map.Entry<String, JsonElement>> entitySet = obj.entrySet();
	            for(Map.Entry<String, JsonElement> entry: entitySet){
	                map.put(entry.getKey(), read(entry.getValue()));
	            }
	            return map;
	        }else if( in.isJsonPrimitive()){
	            JsonPrimitive prim = in.getAsJsonPrimitive();
	            if(prim.isBoolean()){
	                return prim.getAsBoolean();
	            }else if(prim.isString()){
	                return prim.getAsString();
	            }else if(prim.isNumber()){
	                Number num = prim.getAsNumber();
	                // here you can handle double int/long values
	                // and return any type you want
	                // this solution will transform 3.0 float to long values
	                if(Math.ceil(num.doubleValue())  == num.longValue())
	                   return num.longValue();
	                else{
	                    return num.doubleValue();
	                }
	            }
	        }
	        return null;
	    }
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
