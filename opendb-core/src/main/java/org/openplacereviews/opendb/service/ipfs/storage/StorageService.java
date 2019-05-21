package org.openplacereviews.opendb.service.ipfs.storage;

import org.openplacereviews.opendb.service.ipfs.pinning.PinningService;
import org.openplacereviews.opendb.util.exception.IPFSNotFoundException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

/**
 * Interface representing a storage layer
 *
 */
public interface StorageService {

	/**
	 * Return the ReplicatSet AppConfiguration (list of services able to pin a file)
	 * @return ReplicaSet   List of PinningServices
	 */
	Set<PinningService> getReplicaSet();

	/**
	 * Write content on the storage layer
	 * @param content InputStream
	 * @return Content ID (hash, CID)
	 */
	String write(InputStream content);

	/**
	 * Write content on the storage layer
	 * @param content Byte array
	 * @return Content ID (hash, CID
	 */
	String write(byte[] content);

	/**
	 * Read content from the storage layer and write it in a ByteArrayOutputStream
	 * @param id Content ID (hash, CID
	 * @return content
	 */
	OutputStream read(String id);

	/**
	 *  Read content from the storage layer and write it the OutputStream provided
	 * @param id Content ID (hash, CID
	 * @param output OutputStream to write content to
	 * @return Outputstream passed as argument
	 */
	OutputStream read(String id, OutputStream output) throws IPFSNotFoundException;
}
