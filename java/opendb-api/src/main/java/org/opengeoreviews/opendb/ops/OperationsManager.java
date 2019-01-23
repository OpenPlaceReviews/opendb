package org.opengeoreviews.opendb.ops;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.opengeoreviews.opendb.api.ApiController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

@Service
public class OperationsManager {

	@Autowired
	public OperationsCache cache;
	
	@Autowired
	public OperationsRegistry registry;
	
	@Autowired
	public JdbcTemplate jdbcTemplate;
	
	public int CURRENT_BLOCK_ID = 0;
	
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }	
	
	
	public OpBlock parseBootstrapBlock(String id) {
		return new Gson().fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
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
