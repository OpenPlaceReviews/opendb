package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;

import org.springframework.core.io.AbstractResource;

/**
 * Data provider for public api. 
 * Manages how to cache and evaluate content
 * @param <T>
 */
public interface IPublicDataProvider<Params, Value> {

	List<Params> getKeysToCache();

	Params formatParams(Map<String, String[]> params);
	
	Value getContent(Params params);

	AbstractResource formatContent(Value content);

	AbstractResource getMetaPage(Map<String, String[]> params);

}
