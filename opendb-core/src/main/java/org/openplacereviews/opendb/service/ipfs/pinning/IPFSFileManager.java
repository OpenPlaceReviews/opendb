package org.openplacereviews.opendb.service.ipfs.pinning;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.ipfs.dto.ImageDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class IPFSFileManager {

	protected static final Log LOGGER = LogFactory.getLog(IPFSFileManager.class);

	@Value("${ipfs.directory:/opendb/storage/}")
	public String DIRECTORY;

	public IPFSFileManager() {

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
	private String getRootDirectoryPath() {
		return System.getProperty("user.home") + "/" + DIRECTORY;
	}

	public Path getFileByCid(String cid, String extension) throws FileNotFoundException {
		String filePath = System.getProperty("user.home") + "/" + DIRECTORY + generateFileName(cid, extension);

		if (Files.exists(Paths.get(filePath))) {
			return Paths.get(filePath);
		} else {
			throw new FileNotFoundException();
		}
	}

	public void addFileToStorage(ImageDTO imageDTO) throws IOException {
		File file = new File(getRootDirectoryPath() + generateFileName(imageDTO.getCid(), imageDTO.getExtension()));

		FileUtils.writeByteArrayToFile(file, imageDTO.getMultipartFile().getBytes());
	}

	public void removeFileFromStorage(ImageDTO imageDTO) {
		File file = new File(getRootDirectoryPath() + generateFileName(imageDTO.getCid(), imageDTO.getExtension()));

		if (file.delete()) {
			LOGGER.debug(file.getName() + " is deleted!");
		} else {
			LOGGER.error("Delete operation is failed.");
		}
	}

	private String generateFileName(String hash, String ext) {
		StringBuilder fPath = generateDirectoryHierarchy(hash);
		fPath.append(hash).append(".").append(ext);

		return fPath.toString();
	}

	private StringBuilder generateDirectoryHierarchy(String hash) {
		byte[] bytes = hash.getBytes();
		StringBuilder fPath = new StringBuilder();

		for (int i = 0; i < 12; i += 4) {
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
