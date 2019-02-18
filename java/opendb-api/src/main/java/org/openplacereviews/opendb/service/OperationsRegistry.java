package org.openplacereviews.opendb.service;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OpenDBOperation;
import org.openplacereviews.opendb.ops.OpenDBOperationExec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.jdbc.core.JdbcTemplate;
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
	
	private static class OperationTypeDefinition {
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
	
	
	@SuppressWarnings("unchecked")
	public OperationsRegistry() {
		LOGGER.info("Scanning for registered operations...");

		ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
		scanner.addIncludeFilter(new AnnotationTypeFilter(OpenDBOperation.class));
		for (BeanDefinition bd : scanner.findCandidateComponents("org.openplacereviews")) {
			try {
				Class<? extends OpenDBOperationExec> cl = (Class<? extends OpenDBOperationExec>) Class.forName(bd.getBeanClassName());
				String op = cl.getAnnotation(OpenDBOperation.class).type() + ":"+cl.getAnnotation(OpenDBOperation.class).name();
				LOGGER.info(String.format("Register op '%s' -> %s ", op, bd.getBeanClassName()));
				// operations.put(op, cl);
			} catch (Exception e) {
				LOGGER.warn(e.getMessage(), e);
			}
		}
		
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_LOGIN), new OperationTypeDefinition(null));
		operations.put(getOperationId(OP_TYPE_SYS, OP_SIGNUP), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_ROLE), new OperationTypeDefinition(null));
		operations.put(getOperationId(OP_TYPE_SYS, OP_DENY), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_TABLE), new OperationTypeDefinition(null));
		
		operations.put(getOperationId(OP_TYPE_SYS, OP_OPERATION), new OperationTypeDefinition(null));
	}
	
	public void triggerEvent(String eventId, JsonObject obj) {
		dbManager.executeMappingOperation(getOperationId(OP_TYPE_EVENT, eventId), obj);
	}


	public OpenDBOperationExec createOperation(OpDefinitionBean def) {
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_TABLE))) {
			dbManager.registerTableDefinition(def);
		}
		if(def.getOperationId().equals(getOperationId(OperationsRegistry.OP_TYPE_SYS, OP_OPERATION))) {
			operations.put(def.getStringValue(F_NAME), new OperationTypeDefinition(def));
			dbManager.registerMappingOperation(def.getStringValue(F_NAME), def);
		}
		OperationTypeDefinition otd = operations.get(def.getOperationId());
		return new OpenDBOperationExec() {
			
			@Override
			public boolean prepare(OpDefinitionBean definition) {
				return otd  == null || otd.validate(definition);
			}
			
			@Override
			public String getDescription() {
				return "";
			}
			
			@Override
			public OpDefinitionBean getDefinition() {
				return def;
			}
			
			@Override
			public boolean execute(JdbcTemplate template) {
				return true;
			}
		};
	}


	
	
	
	
}
