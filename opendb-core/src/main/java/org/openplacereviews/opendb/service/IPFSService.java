package org.openplacereviews.opendb.service;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.dto.IpfsStatusDTO;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.TechnicalException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.multihash.Multihash;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

@Service
public class IPFSService {

	protected static final Log LOGGER = LogFactory.getLog(IPFSService.class);
	
	public static final String BASE_URI = "%s://%s:%s/";

	@Value("${opendb.storage.ipfs.node.host:}")
	public String ipfsHost;

	@Value("${opendb.storage.ipfs.node.port:5001}")
	public int ipfsPort;
	
	@Value("${opendb.storage.ipfs.node.readTimeoutMs:10000}")
	public int ipfsReadTimeoutMs;

	private IPFS ipfs;
	private ExecutorService pool;
	private RetryPolicy<Object> retryPolicy;

	public void connect() throws IOException {
		if (!OUtils.isEmpty(ipfsHost)) {
			ipfs = new IPFS(ipfsHost, ipfsPort);
			configureThreadPool(10);
			configureRetry(1, Duration.ofSeconds(1));
			LOGGER.info(String.format("Connected to ipfs [host: %s, port: %d]: Node v.%s", ipfsHost, ipfsPort,
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
	public IpfsStatusDTO getIpfsNodeInfo() throws IOException, UnirestException {
		HttpResponse<String> response = Unirest.get(String.format(BASE_URI + "api/v0/id", ipfs.protocol, ipfs.host, ipfs.port)).asString();
		TreeMap objectTreeMap = new Gson().fromJson(response.getBody(), TreeMap.class);
		return new IpfsStatusDTO()
				.setStatus("CONNECTED")
				.setPeerId(ipfs.config.get("Identity.PeerID").toString())
				.setVersion(objectTreeMap.get("AgentVersion").toString())
				.setGateway(ipfs.config.get("Addresses.Gateway").toString())
				.setApi(ipfs.config.get("Addresses.API").toString())
				.setAddresses(objectTreeMap.get("Addresses").toString())
				.setPublicKey(objectTreeMap.get("PublicKey").toString());
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
					Map<Multihash, Object> cids = this.ipfs.pin.ls(IPFS.PinType.recursive);
					return cids.entrySet().stream()
							.map(e-> e.getKey().toBase58())
							.collect(Collectors.toList());
				});
	}
	
	public List<String> getTrackedResources() {
		// TODO return list of tracked resources so the files could be reuploaded
		return new ArrayList<String>();
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
						byte[] content = ipfsFetcherResult.get(this.ipfsReadTimeoutMs, TimeUnit.MILLISECONDS);
						IOUtils.write(content, output);
						return output;
					} catch (java.util.concurrent.TimeoutException ex) {
						LOGGER.error(String.format("Timeout Exception while fetching file from IPFS [id: %s, timeout: %d ms]", id,
								ipfsReadTimeoutMs));
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


}
