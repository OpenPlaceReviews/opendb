package org.opengeoreviews.opendb.api;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.opengeoreviews.opendb.SecUtils;
import org.opengeoreviews.opendb.ops.OpBlock;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class BlocksFormatting {

	private Gson gson;
	
	public BlocksFormatting() {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(OpDefinitionBean.class, new OpDefinitionBean.OpDefinitionBeanAdapter());
		gson = builder.create();
	}
	
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }
	
	public OpBlock parseBootstrapBlock(String id) {
		return gson.fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
	}

	
	public void calculateOperationHash(OpDefinitionBean ob) {
		ob.remove(OpDefinitionBean.F_HASH);
		String hash = SecUtils.calculateSha1(gson.toJson(ob));
		ob.putStringValue(OpDefinitionBean.F_HASH, hash);		
	}

	public String toJson(OpBlock bl) {
		return gson.toJson(bl);
	}


}
