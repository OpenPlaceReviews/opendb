package org.openplacereviews.opendb.service.ipfs.pinning;

import java.util.List;

/**
 * Interface representing a pinning service.
 *
 */
public interface PinningService {

	/**
	 * Pin content
	 * @param id Content ID (hash, CID)
	 */
	void pin(String id);

	/**
	 * unpin content
	 * @param id Content ID (hash, CID)
	 */
	void unpin(String id);

	/**
	 * Get list of all tracked files
	 * @return List of Content ID
	 */
	List<String> getTracked();
}
