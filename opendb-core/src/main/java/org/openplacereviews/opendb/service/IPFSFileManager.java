package org.openplacereviews.opendb.service;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.dto.IpfsStatusDTO;
import org.openplacereviews.opendb.dto.ResourceDTO;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.OUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class IPFSFileManager {

	protected static final Log LOGGER = LogFactory.getLog(IPFSFileManager.class);

	private static final int SPLIT_FOLDERS_DEPTH = 3;
	private static final int FOLDER_LENGTH = 4;

	@Value("${opendb.storage.local-storage:}")
	private String directory;

	@Value("${opendb.storage.timeToStoreUnusedSec:86400}")
	private int timeToStoreUnusedObjectsSeconds;

	private File folder;

	@Autowired
	private DBConsensusManager dbManager;

	@Autowired
	private IPFSService ipfsService;

	public void init() {
		try {
			if (!OUtils.isEmpty(directory)) {
				folder = new File(directory);
				folder.mkdirs();
				ipfsService.connect();
				LOGGER.info(String.format("Init directory to store external images at %s", folder.getAbsolutePath()));
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("IPFS directory for images was not created");
		}
	}

	public boolean isRunning() {
		return folder != null;
	}

	public boolean isIPFSRunning() {
		return ipfsService.isRunning();
	}

	public ResourceDTO addFile(ResourceDTO resourceDTO) throws IOException {
		resourceDTO.setHash(SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, resourceDTO.getMultipartFile().getBytes()));
		if (isIPFSRunning()) {
			resourceDTO.setCid(ipfsService.writeContent(resourceDTO.getMultipartFile().getBytes()));
		}
		File f = getFileByHash(resourceDTO.getHash(), resourceDTO.getExtension());
		FileUtils.writeByteArrayToFile(f, resourceDTO.getMultipartFile().getBytes());
		dbManager.storeResourceObject(resourceDTO);
		return resourceDTO;
	}


	public File getFileByHash(String hash, String extension) throws FileNotFoundException {
		return getFileByHashImpl(hash, extension);
	}


	public List<ResourceDTO> getMissingImagesInIPFS() {
		List<String> pinnedImagesOnIPFS = ipfsService.getPinnedResources();
		List<ResourceDTO> activeResources = dbManager.getResources(true, 0);
		List<ResourceDTO> missingResources = new ArrayList<ResourceDTO>();
		activeResources.forEach(r -> {
			if (!pinnedImagesOnIPFS.contains(r.getCid())) {
				missingResources.add(r);
			}
		});
		return missingResources;
	}


	public IpfsStatusDTO getCurrentStatus(boolean full) throws IOException, UnirestException {
		IpfsStatusDTO stat = ipfsService.getIpfsNodeInfo();

		if (full) {
			stat.setAmountDBResources(dbManager.getAmountResourcesInDB());
			stat.setMissingResources(getMissingImagesInIPFS());
			stat.setDeprecatedResources(dbManager.getResources(false, timeToStoreUnusedObjectsSeconds));
		}
		return stat;
	}

	public List<ResourceDTO> uploadMissingResourcesToIPFS() {
		List<ResourceDTO> missingResources = getMissingImagesInIPFS();
		missingResources.forEach(r -> {
			try {
				LOGGER.info("Start uploading image from/to node for cid: " + r.getCid() + " " + r.getHash());
				File f = getFileByHash(r.getHash(), r.getExtension());
				FileInputStream fis = new FileInputStream(f);
				String newCid = ipfsService.writeContent(fis);
				fis.close();
				LOGGER.info("Uploaded with cid: " + newCid);
				if (!OUtils.equals(newCid, r.getCid())) {
					// ?????
					LOGGER.error(String.format("CID mismatch ! %s != %s for %s.", newCid, r.getCid(), r.getHash()));
				}
				ipfsService.pin(newCid);
			} catch (Exception e) {
				LOGGER.error(String.format("Error while pinning missing object [hash: %s, cid: %s] : %s",
						r.getCid(), r.getHash(), e.getMessage()), e);
			}
		});

		return missingResources;
	}

	public List<ResourceDTO> removeUnusedImageObjectsFromSystemAndUnpinningThem() throws IOException {
		List<ResourceDTO> notActiveImageObjects = dbManager.getResources(false, timeToStoreUnusedObjectsSeconds);
		notActiveImageObjects.parallelStream().forEach(res -> {
			try {
				removeImageObject(res);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		});
		ipfsService.clearNotPinnedImagesFromIPFSLocalStorage();
		return notActiveImageObjects;
	}

	private void removeImageObject(ResourceDTO resourceDTO) throws FileNotFoundException {
		ipfsService.unpin(resourceDTO.getCid());
		dbManager.removeResObjectFromDB(resourceDTO);
		File file = getFileByHash(resourceDTO.getHash(), resourceDTO.getExtension());
		if (file.delete()) {
			LOGGER.info(String.format("File %s is deleted", file.getName()));
		} else {
			LOGGER.error(String.format("Deleting %s has failed", file.getAbsolutePath()));
		}
	}

	public void processOperations(List<OpOperation> candidates) {
		List<ResourceDTO> array = new ArrayList<ResourceDTO>();
		candidates.forEach(operation -> {
			List<OpObject> nw = operation.getNew();
			for (OpObject o : nw) {
				array.clear();
				getImageObject(o.getRawOtherFields(), array);
				array.forEach(resDTO -> {
					dbManager.updateImageActiveStatus(resDTO, true);
					ipfsService.pin(resDTO.getCid());
				});
			}
		});
	}

	@SuppressWarnings("unchecked")
	private void getImageObjectFromList(List<?> l, List<ResourceDTO> array) {
		for (Object o : l) {
			if (o instanceof Map) {
				getImageObject((Map<String, ?>) o, array);
			}
			if (o instanceof List) {
				getImageObjectFromList((List<?>) o, array);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void getImageObject(Map<String, ?> map, List<ResourceDTO> array) {
		if (map.containsKey(OpOperation.F_TYPE) && map.get(OpOperation.F_TYPE).equals("#image")) {
			array.add(ResourceDTO.of(map.get("hash").toString(), map.get("extension").toString(), map.get("cid").toString()));
		} else {
			map.entrySet().forEach(e -> {
				if (e.getValue() instanceof Map) {
					getImageObject((Map<String, ?>) e.getValue(), array);
				}
				if (e.getValue() instanceof List) {
					getImageObjectFromList((List<?>) e.getValue(), array);
				}
			});
		}
	}

	private File getFileByHashImpl(String hash, String extension) throws FileNotFoundException {
		String fileName = generateFileDirAndName(hash, extension);
		File file = new File(folder, fileName);
		if (extension == null) {
			File[] listOfFiles = file.listFiles();

			if (listOfFiles != null) {
				for (File listOfFile : listOfFiles) {
					if (FilenameUtils.getBaseName(listOfFile.getName()).equals(hash)) {
						return listOfFile;
					}
				}
			}

			throw new FileNotFoundException("File with hash: " + hash + " was not found");
		} else {
			return file;
		}
	}


	private String generateFileDirAndName(String hash, String ext) {
		StringBuilder fPath = new StringBuilder();
		String thash = hash;
		int splitF = SPLIT_FOLDERS_DEPTH;
		while (splitF > 0 && thash.length() > FOLDER_LENGTH) {
			fPath.append(thash.substring(0, FOLDER_LENGTH)).append("/");
			thash = thash.substring(FOLDER_LENGTH);
			splitF--;
		}
		if (ext != null) {
			File dir = new File(folder, fPath.toString());
			dir.mkdirs();
			fPath.append(hash).append(".").append(ext);
		}
		return fPath.toString();
	}


}
