package org.openplacereviews.opendb.service;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class BlocksFormatting {

	private Gson gson;
	
	public BlocksFormatting() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpDefinitionBean.class, new OpDefinitionBean.OpDefinitionBeanAdapter());
		gson = builder.create();
	}
	
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }
	
	public OpBlock parseBootstrapBlock(String id) {
		return gson.fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
	}
	
	public OpDefinitionBean parseOperation(String opJson) {
		return gson.fromJson(opJson, OpDefinitionBean.class);
	}

	
	public String calculateOperationHash(OpDefinitionBean ob, boolean set) {
		String oldHash = (String) ob.remove(OpDefinitionBean.F_HASH);
		Object sig = ob.remove(OpDefinitionBean.F_SIGNATURE);
		
		String hash = "sha256:" + SecUtils.calculateSha256(gson.toJson(ob));
		if(set) {
			ob.putStringValue(OpDefinitionBean.F_HASH,  hash);
		} else {
			ob.putStringValue(OpDefinitionBean.F_HASH, oldHash);
		}
		ob.putObjectValue(OpDefinitionBean.F_SIGNATURE, sig);
		return hash;
	}
	
	public String toValidateSignatureJson(OpDefinitionBean op) {
		Object sig = op.remove(OpDefinitionBean.F_SIGNATURE);
		String json = gson.toJson(op);
		op.putObjectValue(OpDefinitionBean.F_SIGNATURE, sig);
		return json;
	}

	public String toJson(OpBlock bl) {
		return gson.toJson(bl);
	}
	
	public String toJson(OpDefinitionBean op) {
		return gson.toJson(op);
	}


}
