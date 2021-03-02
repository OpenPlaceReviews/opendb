package org.openplacereviews.opendb.service;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.multihash.Multihash;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.IPFSFileManager.IpfsStatusDTO;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.TechnicalException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.ipfs.api.IPFS.PinType.recursive;

@Service
public class IPFSService {

	public static final String BASE_URI = "%s://%s:%s/";
	protected static final Log LOGGER = LogFactory.getLog(IPFSService.class);

	private IPFS ipfs;
	private ExecutorService pool;
	private RetryPolicy<Object> retryPolicy;

	@Autowired
	private SettingsManager settingsManager;

	public void connect() throws IOException {
		if (!OUtils.isEmpty(getIpfsHost())) {
			ipfs = new IPFS(getIpfsHost(), getIpfsPort());
			configureThreadPool(10);
			configureRetry(1, Duration.ofSeconds(1));
			LOGGER.info(String.format("Connected to ipfs [host: %s, port: %d]: Node v.%s", getIpfsHost(), getIpfsPort(),
					ipfs.version()));
		}
	}

	public IPFSService configureThreadPool(Integer poolSize) {
		this.pool = Executors.newFixedThreadPool(poolSize);
		return this;
	}


	public IPFSService configureRetry(Integer maxRetry, Duration delay) {
		this.retryPolicy = new RetryPolicy<>()
				.handle(IOException.class)
				.handle(ExecutionException.class)
				.handle(TimeoutException.class)
				.withDelay(delay)
				.withMaxRetries(maxRetry);
		return this;
	}

	public boolean isRunning() {
		return ipfs != null;
	}


	public void clearNotPinnedImagesFromIPFSLocalStorage() throws IOException {
		this.ipfs.repo.gc();
	}

	@SuppressWarnings("rawtypes")
	public IpfsStatusDTO getIpfsNodeInfo() throws IOException {
		Gson gson = new Gson();
		TreeMap objectTreeMap = gson.fromJson(requestIPFSFromApi("api/v0/id"), TreeMap.class);
		IpfsStatusDTO ipfsStatusDTO = new IpfsStatusDTO()
				.setStatus("CONNECTED")
				.setPeerId(ipfs.config.get("Identity.PeerID").toString())
				.setVersion(objectTreeMap.get("AgentVersion").toString())
				.setGateway(ipfs.config.get("Addresses.Gateway").toString())
				.setApi(ipfs.config.get("Addresses.API").toString())
				.setAddresses(objectTreeMap.get("Addresses").toString())
				.setPublicKey(objectTreeMap.get("PublicKey").toString());

		JsonObject jsonObject = gson.fromJson(requestIPFSFromApi("api/v0/repo/stat"), JsonObject.class);
		ipfsStatusDTO
				.setRepoSize(jsonObject.get("RepoSize").getAsBigDecimal())
				.setStorageMax(jsonObject.get("StorageMax").getAsBigDecimal())
				.setAmountIpfsResources(jsonObject.get("NumObjects").getAsBigDecimal())
				.setRepoPath(jsonObject.get("RepoPath").getAsString());

		jsonObject = gson.fromJson(requestIPFSFromApi("api/v0/diag/sys"), JsonObject.class);
		ipfsStatusDTO
				.setDiskInfo(jsonObject.get("diskinfo").toString())
				.setMemory(jsonObject.get("memory").toString())
				.setRuntime(jsonObject.get("runtime").toString())
				.setNetwork(jsonObject.get("net").toString());

		ipfsStatusDTO.setAmountPinnedIpfsResources(ipfs.pin.ls(recursive).size());
		return ipfsStatusDTO;
	}

	private String requestIPFSFromApi(String api) {
		String s = "{}", line;
		try {
			String url = String.format(BASE_URI + api, ipfs.protocol, ipfs.host, ipfs.port);
			HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("POST");
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

			while ((line = in.readLine()) != null) {
				if (s.length() == "{}".length()) {
					s = line;
				} else {
					s += "\n" + line;
				}
			}
			return s;
		} catch (Exception e) {
			s = String.format("{'error':'%s'}", e.getMessage());
			LOGGER.error(e.getMessage(), e);
			return s;
		}

	}

	public String writeContent(InputStream content) {
		try {
			return writeContent(IOUtils.toByteArray(content));
		} catch (IOException ex) {
			throw new TechnicalException("Exception converting Inputstream to byte array", ex);
		}
	}

	public String writeContent(byte[] content) {
		LOGGER.debug("Write file on IPFS");
		return Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error(String.format("Exception writting file on IPFS after %d attemps. %s",
						event.getAttemptCount(), event.getResult())))
				.onSuccess(event -> LOGGER.debug(String.format("File written on IPFS: hash=%s ", event.getResult())))
				.get(() -> {
					NamedStreamable.ByteArrayWrapper file = new NamedStreamable.ByteArrayWrapper(content);
					MerkleNode response = this.ipfs.add(file).get(0);
					return response.hash.toString();
				});
	}

	public boolean pin(String cid) {
		LOGGER.debug(String.format("Pin CID %s on IPFS", cid));
		try {
			Failsafe.with(retryPolicy)
					.onFailure(event -> LOGGER.error(String.format("Exception pinning cid %s on IPFS after %d attemps", cid, event.getAttemptCount())))
					.onSuccess(event -> LOGGER.debug(String.format("CID %s pinned on IPFS", cid)))
					.run(() -> {
						Multihash hash = Multihash.fromBase58(cid);
						this.ipfs.pin.add(hash);
					});

			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public boolean unpin(String cid) {
		LOGGER.debug(String.format("Unpin CID %s on IPFS", cid));
		try {
			Failsafe.with(retryPolicy)
					.onFailure(event -> LOGGER.error(String.format("Exception unpinning cid %s on IPFS after %d attemps", cid, event.getAttemptCount())))
					.onSuccess(event -> LOGGER.debug(String.format("CID %s unpinned on IPFS", cid)))
					.run(() -> {
						Multihash hash = Multihash.fromBase58(cid);
						this.ipfs.pin.rm(hash);
					});
			return true;
		} catch (Exception e) {
			return false;
		}

	}


	public List<String> getPinnedResources() {
		return Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error(String.format("Exception getting pinned files on IPFS after %d attemps", event.getAttemptCount())))
				.onSuccess(event -> LOGGER.debug(String.format("Get pinned files on IPFS: %s", event.getResult())))
				.get(() -> {
					Map<Multihash, Object> cids = this.ipfs.pin.ls(recursive);
					return cids.entrySet().stream()
							.map(e -> e.getKey().toBase58())
							.collect(Collectors.toList());
				});
	}

	public OutputStream read(String id, OutputStream output) {
		LOGGER.debug(String.format("Read file on IPFS [id: %s]", id));

		return Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error(String.format("Exception reading file [id: %s] on IPFS after %d attemps. %s", id, event.getAttemptCount(), event.getResult())))
				.onSuccess(event -> LOGGER.debug(String.format("File read on IPFS: [id: %s] ", id)))
				.get(() -> {
					try {
						Multihash filePointer = Multihash.fromBase58(id);
						Future<byte[]> ipfsFetcherResult = pool.submit(new IPFSContentFetcher(ipfs, filePointer));
						byte[] content = ipfsFetcherResult.get(getIpfsReadTimeoutMs(), TimeUnit.MILLISECONDS);
						IOUtils.write(content, output);
						return output;
					} catch (java.util.concurrent.TimeoutException ex) {
						LOGGER.error(String.format("Timeout Exception while fetching file from IPFS [id: %s, timeout: %d ms]", id,
								getIpfsReadTimeoutMs()));
						throw new TimeoutException("Timeout Exception while fetching file from IPFS [id: " + id + "]");
					} catch (Exception ex) {
						LOGGER.error(String.format("Execution Exception while fetching file from IPFS [id: %s]", id), ex);
						throw new TechnicalException("Execution Exception while fetching file from IPFS [id: " + id + "]", ex);
					}
				});

	}

	private class IPFSContentFetcher implements Callable<byte[]> {

		private final IPFS ipfs;
		private final Multihash multihash;

		public IPFSContentFetcher(IPFS ipfs, Multihash multihash) {
			this.ipfs = ipfs;
			this.multihash = multihash;
		}

		@Override
		public byte[] call() {
			try {
				return this.ipfs.cat(multihash);
			} catch (IOException ex) {
				LOGGER.error(String.format("Exception while fetching file from IPFS [hash: %s]", multihash), ex);
				throw new TechnicalException("Exception while fetching file from IPFS " + multihash, ex);
			}
		}
	}

	public int getIpfsPort() {
		return settingsManager.OPENDB_STORAGE_IPFS_NODE_PORT.get();
	}

	public String getIpfsHost() {
		return settingsManager.OPENDB_STORAGE_IPFS_NODE_HOST.get();
	}

	public int getIpfsReadTimeoutMs() {
		return settingsManager.OPENDB_STORAGE_IPFS_NODE_READ_TIMEOUT_MS.get();
	}
	
	
	public static class ResourceDTO {

		private String type = "#image";
		private String hash;
		private String extension;
		private String cid;
		private transient Date added;

		private transient MultipartFile multipartFile;

		public static ResourceDTO of(MultipartFile multipartFile) {
			ResourceDTO imageDTO = new ResourceDTO();
			imageDTO.extension = FilenameUtils.getExtension(multipartFile.getOriginalFilename());
			imageDTO.multipartFile = multipartFile;

			return imageDTO;
		}

		public static ResourceDTO of(String hash, String extension, String cid) {
			ResourceDTO imageDTO = new ResourceDTO();
			imageDTO.hash = hash;
			imageDTO.extension = extension;
			imageDTO.cid = cid;

			return imageDTO;
		}

		public String getType() {
			return type;
		}

		public String setType() {
			return type;
		}

		public String getHash() {
			return hash;
		}

		public void setHash(String hash) {
			this.hash = hash.substring(hash.lastIndexOf(':') + 1);
		}

		public String getExtension() {
			return extension;
		}

		public void setExtension(String extension) {
			this.extension = extension;
		}

		public String getCid() {
			return cid;
		}

		public void setCid(String cid) {
			this.cid = cid;
		}

		public Date getAdded() {
			return added;
		}

		public void setAdded(Date added) {
			this.added = added;
		}

		public MultipartFile getMultipartFile() {
			return multipartFile;
		}

		public void setMultipartFile(MultipartFile multipartFile) {
			this.multipartFile = multipartFile;
		}
	}
}
