package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;

import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.springframework.core.io.AbstractResource;

/**
 * Data provider for public api. 
 * Manages how to cache and evaluate content
 * @param <Params>
 */
public interface IPublicDataProvider<Params, Value> {

	
	/**
	 * Keys to be reevaluated by cache
	 * In this method provider could decide whether cache needs to be cleaned or maintained
	 * @return keys 
	 */
	List<Params> getKeysToCache(PublicAPIEndpoint<Params, Value> api);

	/**
	 * Parse object params from HTTP params 
	 * @param params
	 * @return object params
	 */
	Params formatParams(Map<String, String[]> params);
	
	/**
	 * Gets API content as an object
	 * @param params
	 * @return any object that will be stored in the cache
	 */
	Value getContent(Params params);

	/**
	 * Formats returned object from {@literal #getContent(Object)} into a HTTP resource (for example JSON)
	 */
	AbstractResource formatContent(Value content);

	/**
	 * Returns index html page for API to test 
	 */
	AbstractResource getMetaPage(Map<String, String[]> params);
	
	/**
	 * Once operation added to the queue (@param block is null) or 
	 * added as part of the block, then method is called to allow 
	 * cache to be reevaluated.
	 * Returns true if something has changed
	 */
	default boolean operationAdded(PublicAPIEndpoint<Params, Value> api, OpOperation op, OpBlock block) {
		return false;
	};
	
	/**
	 * Serialize methods could be used to store cache in database
	 */
	default String serializeValue(Value v) {
		// not serializable
		return null;
	};
	
	default Value deserializeValue(String key) {
		// not serializable
		return null;
	}

}
