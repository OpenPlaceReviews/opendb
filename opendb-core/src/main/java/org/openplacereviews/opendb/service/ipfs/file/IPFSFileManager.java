package org.openplacereviews.opendb.service.ipfs.file;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class IPFSFileManager {

	private static final Logger LOGGER = LogManager.getLogger(IPFSFileManager.class);

	private String DIRECTORY = "/opendb/storage/";

	public void init(String directory) {
		try {
			DIRECTORY = directory;
			Files.createDirectories(Paths.get(getRootDirectoryPath()));
			LOGGER.debug("IPFS directory for images was created");
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("IPFS directory for images was not created");
		}
	}

	public void init() {
		try {
			Files.createDirectories(Paths.get(getRootDirectoryPath()));
			LOGGER.debug("IPFS directory for images was created");
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("IPFS directory for images was not created");
		}
	}

	/**
	 * @return root directory for images.
	 */
	public String getRootDirectoryPath() {
		return System.getProperty("user.home") + "/" + DIRECTORY;
	}

	public void addFileToStorage(String hash, MultipartFile mFile) throws IOException {
		File file = new File(getRootDirectoryPath() + generateFileName(hash, FilenameUtils.getExtension(mFile.getOriginalFilename())));

		FileUtils.writeByteArrayToFile(file, mFile.getBytes());
	}

	public String generateFileName(String hash, String ext) {
		StringBuilder fPath = generateDirectoryHierarchy(hash);
		fPath.append(hash).append(".").append(ext);

		return fPath.toString();
	}

	private StringBuilder generateDirectoryHierarchy(String hash) {
		byte[] bytes = hash.getBytes();
		StringBuilder fPath = new StringBuilder();

		for(int i = 0; i < 12; i+=4) {
			fPath.append(new String(bytes, i, 4)).append("/");
		}

		try {
			Files.createDirectories(Paths.get(getRootDirectoryPath() + fPath.toString()));
			LOGGER.debug("Image directory for cid : " + hash + " was created");
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("Image directory was not created");
		}

		return fPath;
	}

}
