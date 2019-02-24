package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.DBConstants;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.ops.OpOperation;
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
	// roles / validation
	public static final String OP_ROLE = "role";
	public static final String OP_GRANT = "grant";
	public static final String OP_VALIDATE = "validate";
	// limit external ops 
	public static final String OP_LIMIT = "limit";
	// ddl
	public static final String OP_TABLE = "table";
	// meta  & mapping
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
	
	@Autowired
	private UsersAndRolesRegistry usersRegistry;
	
	protected static class OperationTypeDefinition {
		public final OpOperation def;
		
		public OperationTypeDefinition(OpOperation def) {
			this.def = def;
		}
		
		public boolean validate(OpOperation op ) {
			return true;
		}
		
		public boolean isBootstrap() {
			return def == null;
		}
	}
	
	public static String getOperationId(String type, String name) {
		return type + ":" + name;
	}
	
	

	private void regDefaultSysOperaitons() {
		operations.put(getOperationId(OP_TYPE_SYS, OP_LOGIN), new OperationTypeDefinition(null));
		operations.put(getOperationId(OP_TYPE_SYS, OP_SIGNUP), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_ROLE), new OperationTypeDefinition(null));
		operations.put(getOperationId(OP_TYPE_SYS, OP_VALIDATE), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_TABLE), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_OPERATION), new OperationTypeDefinition(null));
	}
	
	public void init(MetadataDb metadataDB) {
		LOGGER.info("... Operations registry. Loading operation definitions ...");
		if(metadataDB.tablesSpec.containsKey(DBConstants.OP_DEFINITIONS_TABLE)) {
			List<OpOperation> ops = dbManager.loadOperations(DBConstants.OP_DEFINITIONS_TABLE);
			for(OpOperation o : ops) {
				String opName = o.getStringValue(F_NAME);
				operations.put(opName, new OperationTypeDefinition(o));
				dbManager.registerMappingOperation(opName, o);
			}
		} else {
			regDefaultSysOperaitons();
		}
		LOGGER.info(String.format("+++ Operations registry. Loaded %d operations.", operations.size()));
	}
	
	public void triggerEvent(String eventId, JsonObject obj) {
		dbManager.executeMappingOperation(getOperationId(OP_TYPE_EVENT, eventId), obj);
	}
	
	
	public void executeOperation(OpOperation ro, JsonObject obj) {
	}
	
	
	public boolean preexecuteOperation(OpOperation def, List<OpOperation> bootstrapOps) {
		OperationTypeDefinition opTypeDef = operations.get(def.getOperationId());
		if(opTypeDef == null) {
			throw new UnsupportedOperationException(String.format("Operation %s is not registered", def.getOperationId()));
		}
		// create tables first for initial block
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_TABLE))) {
			dbManager.registerTableDefinition(def, true);
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_OPERATION))) {
			operations.put(def.getStringValue(F_NAME), new OperationTypeDefinition(def));
			dbManager.registerMappingOperation(def.getStringValue(F_NAME), def);
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_SIGNUP))) {
			usersRegistry.getBlockUsers().addAuthOperation(o);
			// TODO add signup operation
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_LOGIN))) {
			// TODO add login operation
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_ROLE))) {
			// TODO add role operation
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_VALIDATE))) {
			// TODO add validate operation
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_GRANT))) {
			// TODO add validate operation
		}
		if (opTypeDef.isBootstrap()) {
			bootstrapOps.add(def);
		} else {
			JsonObject obj = null;
			dbManager.executeMappingOperation(def.getOperationId(), obj);
			triggerEvent(OperationsRegistry.OP_OPERATION, obj);		
		}
		
		return operations.containsKey(def.getOperationId());
	}



	
	
}
