package org.opengeoreviews.opendb.ops;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Service;

@Service
public class OperationsRegistry {

	public static final int VERSION = 1;
	protected static final Log LOGGER = LogFactory.getLog(OperationsRegistry.class);
	
	public static final String OP_TYPE_DDL = "ddl";
	public static final String OP_TYPE_AUTH = "auth";
	
	Map<String, Class<? extends IOpenDBOperation>> operations = new TreeMap<>();
	
	@SuppressWarnings("unchecked")
	public OperationsRegistry() {
		LOGGER.info("Scanning for registered operations...");

		ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
		scanner.addIncludeFilter(new AnnotationTypeFilter(OpenDBOperation.class));
		for (BeanDefinition bd : scanner.findCandidateComponents("org.opengeoreviews")) {
			try {
				Class<? extends IOpenDBOperation> cl = (Class<? extends IOpenDBOperation>) Class.forName(bd.getBeanClassName());
				String op = cl.getAnnotation(OpenDBOperation.class).value();
				LOGGER.info(String.format("Register op '%s' -> %s ", op, bd.getBeanClassName()));
				operations.put(op, cl);
			} catch (Exception e) {
				LOGGER.warn(e.getMessage(), e);
			}
		}
	}

	public IOpenDBOperation createOperation(OpDefinitionBean def) {
		Class<? extends IOpenDBOperation> cl = operations.get(def.getOperationName());
		if(cl != null) {
			try {
				return cl.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				LOGGER.warn(e.getMessage(), e);
				return null;
			}
		}
		return null;
	}
	
	
	
}
