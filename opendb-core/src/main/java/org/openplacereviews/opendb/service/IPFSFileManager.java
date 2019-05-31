package org.openplacereviews.opendb.service;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.dto.IpfsStatusDTO;
import org.openplacereviews.opendb.dto.ResourceDTO;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.TechnicalException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import net.jodah.failsafe.FailsafeException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class IPFSFileManager {

	protected static final Log LOGGER = LogFactory.getLog(IPFSFileManager.class);

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
			if(!OUtils.isEmpty(directory)) {
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
		if(ipfsService.isRunning()) {
			resourceDTO.setCid(ipfsService.writeContent(resourceDTO.getMultipartFile().getBytes()));
		}
		FileUtils.writeByteArrayToFile(getFile(resourceDTO), resourceDTO.getMultipartFile().getBytes());
		dbManager.storeImageObject(resourceDTO);
		return resourceDTO;
	}

	private void removeImageObject(ResourceDTO imageDTO) {
		dbManager.removeImageObjectFromDB(imageDTO);
		File file = getFile(imageDTO);
		if (file.delete()) {
			LOGGER.info(String.format("File %s is deleted", file.getName()));
		} else {
			LOGGER.error(String.format("Deleteing %s has failed", file.getAbsolutePath()));
		}
	}

	private OutputStream read(String cid) {
		try {
			return read(cid, new ByteArrayOutputStream());
		} catch (FailsafeException e) {
			ResourceDTO resourceDTO = dbManager.loadResourceObjectIfExist(cid);
			if (resourceDTO == null) {
				throw new TechnicalException(e.getMessage(), e);
			}

			try {
				Path filepath = ipfsFileManager.getFileByCid(resourceDTO.getCid(), resourceDTO.getExtension());
				String returnedCid = write(Files.readAllBytes(filepath));
				assert returnedCid.equals(cid);
			} catch (Exception e1) {
				throw new TechnicalException("Execution exception while adding new file to IPFS [id: " + cid + "]", e1);
			}

			return read(cid, new ByteArrayOutputStream());
		}
	}
	
	private void getImageObject(Map map, List<ResourceDTO> array) {
		if (map.containsKey(F_TYPE) && map.get(F_TYPE).equals("#image")) {
			array.add(ResourceDTO.of(map.get("hash").toString(), map.get("extension").toString(), map.get("cid").toString()));
		} else {
			map.keySet().forEach(key -> {
				if (map.get(key) instanceof Map) {
					getImageObject( (Map) map.get(key), array);
				}
				if (map.get(key) instanceof List) {
					if (!(((List) map.get(key)).isEmpty()) && !(((List) map.get(key)).get(0).getClass().equals(String.class))) {
						getImageObject( (List<Map>)map.get(key), array);
					}
				}
			});
		}
	}

	private void getImageObject(List<Map> list, List<ResourceDTO> array) {
		for (Map map : list) {
			map.keySet().forEach(key -> {
				if (key.equals(F_TYPE) && map.get(key).equals("#image")) {
					array.add(ResourceDTO.of(map.get("hash").toString(), map.get("extension").toString(), map.get("cid").toString()));
				} else {
					if (map.get(key) instanceof Map) {
						getImageObject( (Map) map.get(key), array);
					}
					if (map.get(key) instanceof List) {
						if (map.get(key).getClass().equals(Map.class)) {
							getImageObject((List<Map>) map.get(key), array);
						}
					}
				}
			});
		}
	}
	

	public IpfsStatusDTO checkingMissingImagesInIPFS() {
		List<String> pinnedImagesOnIPFS = getTrackedResources();
		List<String> activeObjects = dbManager.loadImageObjectsByActiveStatus(true);

		AtomicBoolean status = new AtomicBoolean(true);
		activeObjects.parallelStream().forEach(cid -> {
			if (!pinnedImagesOnIPFS.contains(cid)) {
				status.set(false);
			}
		});

		return IpfsStatusDTO.getMissingImageStatus(activeObjects.size() + "/" + pinnedImagesOnIPFS.size(), status.get() ? "OK" : "NOT OK");
	}

	public IpfsStatusDTO uploadMissingResourcesToIPFS() {
		List<String> pinnedImagesOnIPFS = getTrackedResources();
		List<String> activeObjects = dbManager.loadImageObjectsByActiveStatus(true);

		LOGGER.debug("Start pinning missing images");
		activeObjects.forEach(cid -> {
			LOGGER.debug("Loadded CID: " + cid);
			if (!pinnedImagesOnIPFS.contains(cid)) {
				try {
					LOGGER.debug("Start reading/upload image from/to node for cid: " + cid);
					OutputStream os = read(cid);
					if (!os.toString().isEmpty()) {
						LOGGER.debug("Start pin cid: " + cid);
						pin(cid);
					}
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		});

		return checkingMissingImagesInIPFS();
	}

	public IpfsStatusDTO statusImagesInDB() {
		// TODO display count active objects, pending objects (objects to gc)
		List<ResourceDTO> notActiveImageObjects = dbManager.loadUnusedImageObject(timeToStoreUnusedObjectsSeconds);
		return IpfsStatusDTO.getMissingImageStatus(String.valueOf(notActiveImageObjects.size()), notActiveImageObjects.size() == 0 ? "OK" : "Images can be removed");
	}

	public IpfsStatusDTO removeUnusedImageObjectsFromSystemAndUnpinningThem() throws IOException {
		List<ResourceDTO> notActiveImageObjects = dbManager.loadUnusedImageObject(timeToStoreUnusedObjectsSeconds);

		notActiveImageObjects.parallelStream().forEach(image -> {
			this.unpin(image.getCid());
			removeImageObject(image);
		});

		dbManager.removeUnusedImageObject(timeToStoreUnusedObjectsSeconds);

		clearNotPinnedImagesFromIPFSLocalStorage();

		return statusImagesInDB();
	}


	
	public void processOperations(List<OpOperation> candidates) {
		candidates.forEach(operation -> {
			if (!operation.getImages().isEmpty()) {
				operation.getImages().forEach(imageDTO -> {
					dbConsensusManager.updateImageActiveStatus(imageDTO, true);
					pin
					getReplicaSet().forEach(cluster -> {
						try {
							cluster.pin(imageDTO.getCid());
						} catch (UnirestException e) {
							LOGGER.error(e.getMessage(), e);
						}
					});
				});
			}
		});		
	}

	public Path getFileByCid(String cid, String extension) throws FileNotFoundException {
		String filePath = getRootDirectoryPath() + generateFileName(cid, extension);

		if (Files.exists(Paths.get(filePath))) {
			return Paths.get(filePath);
		} else {
			throw new FileNotFoundException();
		}
	}


	private File getFile(ResourceDTO resDTO) {
		File file = new File(folder + generateFileName(resDTO.getCid(), resDTO.getExtension()));
		return file;
	
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
