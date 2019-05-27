package org.openplacereviews.opendb.service.ipfs.pinning;

import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.List;

/**
 * Interface representing a pinning service.
 */
public interface PinningService {

	/**
	 * Pin content
	 *
	 * @param id Content ID (hash, CID)
	 */
	boolean pin(String id) throws UnirestException;

	/**
	 * Unpin content
	 *
	 * @param id Content ID (hash, CID)
	 */
	boolean unpin(String id) throws UnirestException;

	/**
	 * Get list of all tracked files
	 *
	 * @return List of Content ID
	 */
	List<String> getTracked();
}
