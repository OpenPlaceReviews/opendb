package org.openplacereviews.opendb.service;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.IPFSService.ResourceDTO;
import org.openplacereviews.opendb.util.OUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class IPFSFileManager {

	protected static final Log LOGGER = LogFactory.getLog(IPFSFileManager.class);

	private static final int SPLIT_FOLDERS_DEPTH = 3;
	private static final int FOLDER_LENGTH = 4;

	private File folder;

	@Autowired
	private DBConsensusManager dbManager;

	@Autowired
	private IPFSService ipfsService;

	@Autowired
	private SettingsManager settingsManager;

	public void init() {
		try {
			if (!OUtils.isEmpty(getDirectory())) {
				folder = new File(getDirectory());
				folder.mkdirs();
				ipfsService.connect();
				LOGGER.info(String.format("Init directory to store external images at %s", folder.getAbsolutePath()));
			}
		} catch (IOException e) {
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

	public File downloadFile(String cid, String hash, String ext) throws IOException {
		String fileName = generateFileDirAndName(hash, ext);
		File file = new File(folder, fileName);
		ipfsService.read(cid, new FileOutputStream(file));
		return file;
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


	public IpfsStatusDTO getCurrentStatus(boolean full) throws IOException {
		IpfsStatusDTO stat = ipfsService.getIpfsNodeInfo();

		if (full) {
			stat.setAmountDBResources(dbManager.getAmountResourcesInDB());
			stat.setMissingResources(getMissingImagesInIPFS());
			stat.setDeprecatedResources(dbManager.getResources(false, getTimeToStoreUnusedObjectsSeconds()));
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
		List<ResourceDTO> notActiveImageObjects = dbManager.getResources(false, getTimeToStoreUnusedObjectsSeconds());
		notActiveImageObjects.parallelStream().forEach(res -> {
			try {
				removeImageObject(res);
			} catch (FileNotFoundException e) {
				LOGGER.error("File with hash " + res.getHash() + " was not found", e);
			}
		});
		ipfsService.clearNotPinnedImagesFromIPFSLocalStorage();
		return notActiveImageObjects;
	}

	private void removeImageObject(ResourceDTO resourceDTO) throws FileNotFoundException {
		ipfsService.unpin(resourceDTO.getCid());
		dbManager.removeResource(resourceDTO);
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
			List<OpObject> nw = operation.getCreated();
			for (OpObject o : nw) {
				array.clear();
				getImageObject(o.getRawOtherFields(), array);
				array.forEach(resDTO -> {
					dbManager.updateResourceActiveStatus(resDTO, true);
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
			fPath.append(hash);
			if(!ext.startsWith(".")) {
				fPath.append(".");	
			}
			fPath.append(ext);
		}
		return fPath.toString();
	}

	public String getDirectory() {
		return settingsManager.OPENDB_STORAGE_LOCAL_STORAGE_PATH.get();
	}

	public int getTimeToStoreUnusedObjectsSeconds() {
		return settingsManager.OPENDB_STORAGE_TIME_TO_STORE_UNUSED_RESOURCE_SEC.get();
	}
	
	public static class IpfsStatusDTO {

		private String objects;
		private String status;
		private String peerId;
		private String version;
		private String gateway;
		private String api;
		private String addresses;
		private String publicKey;

		// STATS
		private List<ResourceDTO> missingResources;
		private List<ResourceDTO> deprecatedResources;
		private double amountDBResources;

		// IPFS Storage
		private BigDecimal repoSize;
		private BigDecimal storageMax;
		private double amountPinnedIpfsResources;
		private BigDecimal amountIpfsResources;
		private String repoPath;

		// DISK/SYSTEM Storage
		private String diskInfo;
		private String memory;
		private String runtime;
		private String network;


		public static IpfsStatusDTO getMissingImageStatus(String objects, String status) {
			IpfsStatusDTO ipfsStatusDTO = new IpfsStatusDTO();
			ipfsStatusDTO.objects = objects;
			ipfsStatusDTO.status = status;

			return ipfsStatusDTO;
		}

		public String getObjects() {
			return objects;
		}

		public void setObjects(String objects) {
			this.objects = objects;
		}

		public String getStatus() {
			return status;
		}

		public IpfsStatusDTO setStatus(String status) {
			this.status = status;
			return this;
		}

		public String getPeerId() {
			return peerId;
		}

		public IpfsStatusDTO setPeerId(String peerId) {
			this.peerId = peerId;
			return this;
		}

		public String getVersion() {
			return version;
		}

		public IpfsStatusDTO setVersion(String version) {
			this.version = version;
			return this;
		}

		public String getGateway() {
			return gateway;
		}

		public IpfsStatusDTO setGateway(String gateway) {
			this.gateway = gateway;
			return this;
		}

		public String getApi() {
			return api;
		}

		public IpfsStatusDTO setApi(String api) {
			this.api = api;
			return this;
		}

		public String getAddresses() {
			return addresses;
		}

		public IpfsStatusDTO setAddresses(String addresses) {
			this.addresses = addresses;
			return this;
		}

		public String getPublicKey() {
			return publicKey;
		}

		public IpfsStatusDTO setPublicKey(String publicKey) {
			this.publicKey = publicKey;
			return this;
		}

		public BigDecimal getRepoSize() {
			return repoSize;
		}

		public IpfsStatusDTO setRepoSize(BigDecimal repoSize) {
			this.repoSize = repoSize;
			return this;
		}

		public BigDecimal getStorageMax() {
			return storageMax;
		}

		public IpfsStatusDTO setStorageMax(BigDecimal storageMax) {
			this.storageMax = storageMax;
			return this;
		}

		public String getRepoPath() {
			return repoPath;
		}

		public IpfsStatusDTO setRepoPath(String repoPath) {
			this.repoPath = repoPath;
			return this;
		}

		public String getNetwork() {
			return network;
		}

		public IpfsStatusDTO setNetwork(String network) {
			this.network = network;
			return this;
		}

		public String getDiskInfo() {
			return diskInfo;
		}

		public IpfsStatusDTO setDiskInfo(String diskInfo) {
			this.diskInfo = diskInfo;
			return this;
		}

		public String getMemory() {
			return memory;
		}

		public IpfsStatusDTO setMemory(String memory) {
			this.memory = memory;
			return this;
		}

		public String getRuntime() {
			return runtime;
		}

		public IpfsStatusDTO setRuntime(String runtime) {
			this.runtime = runtime;
			return this;
		}

		public List<ResourceDTO> getMissingResources() {
			return missingResources;
		}

		public void setMissingResources(List<ResourceDTO> missingResources) {
			this.missingResources = missingResources;
		}

		public List<ResourceDTO> getDeprecatedResources() {
			return deprecatedResources;
		}

		public void setDeprecatedResources(List<ResourceDTO> deprecatedResources) {
			this.deprecatedResources = deprecatedResources;
		}

		public double getAmountDBResources() {
			return amountDBResources;
		}

		public void setAmountDBResources(double amountDBResources) {
			this.amountDBResources = amountDBResources;
		}

		public double getAmountPinnedIpfsResources() {
			return amountPinnedIpfsResources;
		}

		public void setAmountPinnedIpfsResources(double amountPinnedIpfsResources) {
			this.amountPinnedIpfsResources = amountPinnedIpfsResources;
		}

		public BigDecimal getAmountIpfsResources() {
			return amountIpfsResources;
		}

		public IpfsStatusDTO setAmountIpfsResources(BigDecimal amountIpfsResources) {
			this.amountIpfsResources = amountIpfsResources;
			return this;
		}
	}
}
