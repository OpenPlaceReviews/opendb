package org.openplacereviews.opendb.service;

import java.util.List;
import java.util.Map;

import org.springframework.core.io.AbstractResource;

/**
 * Data provider for public api. 
 * Manages how to cache and evaluate content
 * @param <T>
 */
public interface IPublicDataProvider<T> {

	List<Map<String, String[]>> getKeysToCache();

	T getContent(Map<String, String[]> params);

	AbstractResource formatContent(T content);

	AbstractResource getPage(Map<String, String[]> params);

}
