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
	
	Map<String, Class<? extends IOpenDBOperation>> operations = new TreeMap<>();
	
	public OperationsRegistry() {
		LOGGER.info("Scanning for registered operations...");
		
		ClassPathScanningCandidateComponentProvider scanner =
				new ClassPathScanningCandidateComponentProvider(true);

		scanner.addIncludeFilter(new AnnotationTypeFilter(OpenDBOperation.class));
			for (BeanDefinition bd : scanner.findCandidateComponents("org.opengeoreviews")) {
				LOGGER.info("Found " + bd.getBeanClassName());
				
			}
	}

	public IOpenDBOperation createOperation(OpDefinitionBean def) {
		Class<? extends IOpenDBOperation> cl = operations.get(def.getName());
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
