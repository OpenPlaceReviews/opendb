package org.openplacereviews.opendb.service;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	public static final String OP_TYPE_SYS  = "sys.";
	// auth
	public static final String OP_LOGIN = OP_TYPE_SYS + "login";
	public static final String OP_SIGNUP = OP_TYPE_SYS + "signup";
	// roles / validation
	public static final String OP_ROLE = OP_TYPE_SYS + "role";
	public static final String OP_GRANT = OP_TYPE_SYS + "grant";
	public static final String OP_VALIDATE = OP_TYPE_SYS + "validate";
	// limit external ops 
	public static final String OP_LIMIT = OP_TYPE_SYS + "limit";
	// ddl?
	public static final String OP_TABLE = OP_TYPE_SYS + "table";
	// meta  & mapping
	public static final String OP_OPERATION = OP_TYPE_SYS + "operation";
	
	
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
		LOGGER.info("... TODO Operations registry. Loading operation definitions ...");
//		if(metadataDB.tablesSpec.containsKey(DBConstants.OP_DEFINITIONS_TABLE)) {
//			List<OpOperation> ops = dbManager.loadOperations(DBConstants.OP_DEFINITIONS_TABLE);
//			for(OpOperation o : ops) {
//				String opName = o.getStringValue(F_NAME);
//				operations.put(opName, new OperationTypeDefinition(o));
//				dbManager.registerMappingOperation(opName, o);
//			}
//		} else {
//			regDefaultSysOperaitons();
//		}
		LOGGER.info(String.format("+++ Operations registry. Loaded %d operations.", operations.size()));
	}
	
	public void triggerEvent(String eventId, JsonObject obj) {
		dbManager.executeMappingOperation(getOperationId(OP_TYPE_EVENT, eventId), obj);
	}
	
	
	public void executeOperation(OpOperation ro, JsonObject obj) {
	}
	
}
