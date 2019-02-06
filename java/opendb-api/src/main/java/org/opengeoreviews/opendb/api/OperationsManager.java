package org.opengeoreviews.opendb.api;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.opengeoreviews.opendb.ops.IOpenDBOperation;
import org.opengeoreviews.opendb.ops.OpBlock;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.opengeoreviews.opendb.ops.OperationsRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class OperationsManager {

	@Autowired
	public OperationsQueue cache;
	
	@Autowired
	public OperationsRegistry registry;
	
	@Autowired
	public JdbcTemplate jdbcTemplate;
	
	public int CURRENT_BLOCK_ID = 0;

	private Gson gson;
	
	public OperationsManager() {
		GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(OpDefinitionBean.class, new OpDefinitionBean.OpDefinitionBeanAdapter());
		gson = builder.create();
	}
	
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }	
	
	
	public Gson getGson() {
		return gson;
	}
	
	
	public OpBlock parseBootstrapBlock(String id) {
		return gson.fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
	}
	

	public void executeBlock(OpBlock block) {
		List<IOpenDBOperation> operations = new ArrayList<IOpenDBOperation>();
		StringBuilder errorMessage = new StringBuilder();
		for(OpDefinitionBean def : block.getOperations()) {
			IOpenDBOperation op = registry.createOperation(def);
			errorMessage.setLength(0); // to be used later
			boolean valid = op != null && op.prepare(def, errorMessage);
			if(valid) { 
				operations.add(op);
			} else {
				// should be informed that operation is not valid
			}
		}
		for(IOpenDBOperation o : operations) {
			errorMessage.setLength(0); // to be used later
			o.execute(jdbcTemplate, errorMessage);
		}
		// serialize and confirm block execution
		CURRENT_BLOCK_ID++;
	}
}
