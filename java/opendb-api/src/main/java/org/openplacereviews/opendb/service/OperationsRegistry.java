package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.DBConstants;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.JsonObject;

@Service
public class OperationsRegistry {

	public static final int VERSION = 1;
	protected static final Log LOGGER = LogFactory.getLog(OperationsRegistry.class);
	
	// system operations
	public static final String OP_TYPE_SYS  = "sys";
	// auth
	public static final String OP_LOGIN = "login";
	public static final String OP_SIGNUP = "signup";
	// validation 
	public static final String OP_ROLE = "role";
	public static final String OP_DENY = "opdeny";
	// mapping
	public static final String OP_TABLE = "table";
	// meta 
	public static final String OP_OPERATION = "operation";
	
	
	// system events
	public static final String OP_TYPE_EVENT = "event";
	public static final String OP_BLOCK = "block";
	public static final String OP_IN_BLOCK = "op_in_block";
	public static final String OP_IN_QUEUE = "op_in_queue";
	
	// op_operation
	private static final String F_NAME = "name";
	
	private Map<String, OperationTypeDefinition> operations = new TreeMap<>();
	
	@Autowired
	private DBDataManager dbManager;
	
	protected static class OperationTypeDefinition {
		public final OpDefinitionBean def;
		
		public OperationTypeDefinition(OpDefinitionBean def) {
			this.def = def;
		}
		
		public boolean validate(OpDefinitionBean op ) {
			return true;
		}
	}
	
	public static String getOperationId(String type, String name) {
		return type + ":" + name;
	}
	
	

	private void regDefaultSysOperaitons() {
		operations.put(getOperationId(OP_TYPE_SYS, OP_LOGIN), new OperationTypeDefinition(null));
		operations.put(getOperationId(OP_TYPE_SYS, OP_SIGNUP), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_ROLE), new OperationTypeDefinition(null));
		operations.put(getOperationId(OP_TYPE_SYS, OP_DENY), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_TABLE), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_OPERATION), new OperationTypeDefinition(null));
	}
	
	public void init(MetadataDb metadataDB) {
		LOGGER.info("... Operations registry. Loading operation definitions ...");
		if(metadataDB.tablesSpec.containsKey(DBConstants.OP_DEFINITIONS_TABLE)) {
			List<OpDefinitionBean> ops = dbManager.loadOperations(DBConstants.OP_DEFINITIONS_TABLE);
			for(OpDefinitionBean o : ops) {
				preexecuteOperation(o);
			}
		} else {
			regDefaultSysOperaitons();
		}
		LOGGER.info(String.format("+++ Operations registry. Loaded %d operations.", operations.size()));
	}
	
	public void triggerEvent(String eventId, JsonObject obj) {
		dbManager.executeMappingOperation(getOperationId(OP_TYPE_EVENT, eventId), obj);
	}
	
	
	public void executeOperation(OpDefinitionBean ro, JsonObject obj) {
		dbManager.executeMappingOperation(ro.getOperationId(), obj);
		triggerEvent(OperationsRegistry.OP_OPERATION, obj);		
	}
	
	
	public boolean preexecuteOperation(OpDefinitionBean def) {
		// create tables first for initial block
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_TABLE))) {
			dbManager.registerTableDefinition(def);
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_OPERATION))) {
			operations.put(def.getStringValue(F_NAME), new OperationTypeDefinition(def));
			dbManager.registerMappingOperation(def.getStringValue(F_NAME), def);
		}
		return operations.containsKey(def.getOperationId());
	}



	
	
}
