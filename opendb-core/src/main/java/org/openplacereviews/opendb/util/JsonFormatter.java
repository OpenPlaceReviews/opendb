package org.openplacereviews.opendb.util;

import com.google.gson.*;

import org.openplacereviews.opendb.dto.ResourceDTO;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.springframework.stereotype.Component;

import java.io.Reader;
import java.lang.reflect.Type;
import java.util.*;

@Component
public class JsonFormatter {

	private Gson gson;
	
	private Gson gsonOperationHash;

	private Gson gsonFullOutput;

	public JsonFormatter() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpOperation.class, new OpOperation.OpOperationBeanAdapter(false));
		builder.registerTypeAdapter(OpObject.class, new OpObject.OpObjectAdapter(false));
		builder.registerTypeAdapter(OpBlock.class, new OpBlock.OpBlockBeanAdapter(false));
		builder.registerTypeAdapter(TreeMap.class, new MapDeserializerDoubleAsIntFix());
		gson = builder.create();
		
		builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpOperation.class, new OpOperation.OpOperationBeanAdapter(false, true));
		builder.registerTypeAdapter(OpObject.class, new OpObject.OpObjectAdapter(false));
		builder.registerTypeAdapter(TreeMap.class, new MapDeserializerDoubleAsIntFix());
		gsonOperationHash = builder.create();
		
		builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpOperation.class, new OpOperation.OpOperationBeanAdapter(true));
		builder.registerTypeAdapter(OpObject.class, new OpObject.OpObjectAdapter(true));
		builder.registerTypeAdapter(OpBlock.class, new OpBlock.OpBlockBeanAdapter(true));
		builder.registerTypeAdapter(TreeMap.class, new MapDeserializerDoubleAsIntFix());
		gsonFullOutput = builder.create();
		
		
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
	public OpOperation parseOperation(String opJson) {
		return gson.fromJson(opJson, OpOperation.class);
	}
	
	public OpObject parseObject(String opJson) {
		return gson.fromJson(opJson, OpObject.class);
	}
	
	public OpBlock parseBlock(String opJson) {
		return gson.fromJson(opJson, OpBlock.class);
	}
	
	public String toJson(OpBlock bl) {
		return gson.toJson(bl);
	}
	
	public JsonElement toJsonElement(Object o) {
		return gson.toJsonTree(o);
	}
	
	@SuppressWarnings("unchecked")
	public TreeMap<String, Object> fromJsonToTreeMap(String json) {
		return gson.fromJson(json, TreeMap.class);
	}
	
	public <T> T fromJson(Reader json, Class<T> classOfT) throws JsonSyntaxException {
		return gson.fromJson(json, classOfT);
	}

	public String imageObjectToJson(ResourceDTO imageDTO) {
		return gson.toJson(imageDTO, ResourceDTO.class);
	}
	
	
	public String fullObjectToJson(Object o) {
		return gsonFullOutput.toJson(o);
	}
	
	
	public String opToJsonNoHash(OpOperation op) {
		return gsonOperationHash.toJson(op);
	}
	
	public String opToJson(OpOperation op) {
		return gson.toJson(op);
	}
	
	public String objToJson(OpObject op) {
		return gson.toJson(op);
	}

	
}
