package org.openplacereviews.opendb.service.ipfs;

import com.google.common.collect.Sets;
import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.multihash.Multihash;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.io.IOUtils;
import org.openplacereviews.opendb.service.ipfs.configuration.IPFSSettings;
import org.openplacereviews.opendb.service.ipfs.pinning.PinningService;
import org.openplacereviews.opendb.service.ipfs.storage.StorageService;
import org.openplacereviews.opendb.util.exception.ConnectionException;
import org.openplacereviews.opendb.util.exception.TechnicalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.openplacereviews.opendb.service.ipfs.configuration.IPFSSettings.DEFAULT_HOST;
import static org.openplacereviews.opendb.service.ipfs.configuration.IPFSSettings.DEFAULT_PORT;

public class IPFSService implements StorageService, PinningService {

	protected static final Logger LOGGER = LoggerFactory.getLogger(IPFSService.class);

	private final IPFSSettings settings;
	private final IPFS ipfs;

	private ExecutorService pool;
	private RetryPolicy<Object> retryPolicy;
	private Set<PinningService> replicaSet;

	private IPFSService(IPFSSettings settings, IPFS ipfs) {
		this.settings = settings;
		this.ipfs = ipfs;
		this.replicaSet = Sets.newHashSet(this); // IPFSService is a PinningService
		this.configureThreadPool(10);
		this.configureRetry(3);
	}

	public static IPFSService connect() {
		return connect(DEFAULT_HOST, DEFAULT_PORT);
	}

	public static IPFSService connect(String host, Integer port) {
		return connect(host, port, null);
	}

	public static IPFSService connect(String multiaddress) {
		return connect(null, null, multiaddress);
	}

	private static IPFSService connect(String host, Integer port, String multiaddress) {
		IPFSSettings settings = IPFSSettings.of(host, port, multiaddress);

		try {
			IPFS ipfs = Optional.ofNullable(multiaddress).map(IPFS::new).orElseGet(() -> new IPFS(host, port));
			LOGGER.info("Connected to ipfs [host: {}, port: {}, multiaddress: {}]: Node v.{}", host, port, multiaddress,
					ipfs.version());

			return new IPFSService(settings, ipfs);

		} catch (Exception ex) {
			String msg = String.format("Error whilst connecting to IPFS [host: %s, port: %s, multiaddress: %s]", host,
					port, multiaddress);

			LOGGER.error(msg, ex);
			throw new ConnectionException(msg, ex);
		}
	}

	public IPFSService configureTimeout(Integer timeout) {
		this.settings.setTimeout(timeout);
		return this;
	}

	public IPFSService configureThreadPool(Integer poolSize) {
		this.pool = Executors.newFixedThreadPool(poolSize);
		return this;
	}

	public IPFSService configureRetry(Integer maxRetry) {
		return this.configureRetry(maxRetry, Duration.ofSeconds(1));
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

	public IPFSService addReplica(PinningService pinningService) {
		this.replicaSet.add(pinningService);
		return this;
	}

	@Override
	public Set<PinningService> getReplicaSet() {
		return replicaSet;
	}

	@Override
	public String write(InputStream content) {

		try {
			return this.write(IOUtils.toByteArray(content));

		} catch (IOException ex) {
			LOGGER.error("Exception converting Inputstream to byte array", ex);
			throw new TechnicalException("Exception converting Inputstream to byte array", ex);
		}
	}

	@Override
	public String write(byte[] content) {
		LOGGER.debug("Write file on IPFS");

		return Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error("Exception writting file on IPFS after {} attemps. {}", event.getAttemptCount(), event.getResult()))
				.onSuccess(event -> LOGGER.debug("File written on IPFS: hash={} ", event.getResult()))
				.get(() -> {
					NamedStreamable.ByteArrayWrapper file = new NamedStreamable.ByteArrayWrapper(content);
					MerkleNode response = this.ipfs.add(file).get(0);
					return response.hash.toString();
				});
	}

	@Override
	public void pin(String cid) {
		LOGGER.debug("Pin CID {} on IPFS", cid);

		Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error("Exception pinning cid {} on IPFS after {} attemps", cid, event.getAttemptCount()))
				.onSuccess(event -> LOGGER.debug("CID {} pinned on IPFS", cid))
				.run(() -> {
					Multihash hash = Multihash.fromBase58(cid);
//					replicaSet.forEach(replica -> {
//						replica.pin(cid);
//					});
					this.ipfs.pin.add(hash);
				});
	}

	@Override
	public void unpin(String cid) {
		LOGGER.debug("Unpin CID {} on IPFS", cid);

		Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error("Exception unpinning cid {} on IPFS after {} attemps", cid, event.getAttemptCount()))
				.onSuccess(event -> LOGGER.debug("CID {} unpinned on IPFS", cid))
				.run(() -> {
					Multihash hash = Multihash.fromBase58(cid);
//					replicaSet.forEach(replica -> {
//						replica.unpin(cid);
//					});
					this.ipfs.pin.rm(hash);
				});
	}

	@Override
	public List<String> getTracked() {
		LOGGER.debug("Get pinned files on IPFS");

		return Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error("Exception getting pinned files on IPFS after {} attemps", event.getAttemptCount()))
				.onSuccess(event -> LOGGER.debug("Get pinned files on IPFS: {}", event.getResult()))
				.get(() -> {
					Map<Multihash, Object> cids = this.ipfs.pin.ls(IPFS.PinType.all);

					return cids.entrySet().stream()
							.map(e-> e.getKey().toBase58())
							.collect(Collectors.toList());
				});
	}

	@Override
	public OutputStream read(String id) {
		return read(id, new ByteArrayOutputStream());
	}

	@Override
	public OutputStream read(String id, OutputStream output) {
		LOGGER.debug("Read file on IPFS [id: {}]", id);

		return Failsafe.with(retryPolicy)
				.onFailure(event -> LOGGER.error("Exception reading file [id: {}] on IPFS after {} attemps. {}", id, event.getAttemptCount(), event.getResult()))
				.onSuccess(event -> LOGGER.debug("File read on IPFS: [id: {}] ", id))
				.get(() -> {
					try {
						Multihash filePointer = Multihash.fromBase58(id);

						Future<byte[]> ipfsFetcherResult = pool.submit(new IPFSContentFetcher(ipfs, filePointer));

						byte[] content = ipfsFetcherResult.get(settings.getTimeout(), TimeUnit.MILLISECONDS);
						IOUtils.write(content, output);

						return output;

					} catch (java.util.concurrent.TimeoutException ex) {
						LOGGER.error("Timeout Exception while fetching file from IPFS [id: {}, timeout: {} ms]", id,
								settings.getTimeout());
						throw new TimeoutException("Timeout Exception while fetching file from IPFS [id: " + id + "]");

					} catch (InterruptedException ex) {
						LOGGER.error("Interrupted Exception while fetching file from IPFS [id: {}]", id);
						Thread.currentThread().interrupt();
						throw new TechnicalException("Interrupted Exception while fetching file from IPFS [id: " + id + "]", ex);

					} catch (ExecutionException ex) {
						LOGGER.error("Execution Exception while fetching file from IPFS [id: {}]", id, ex);
						throw new TechnicalException("Execution Exception while fetching file from IPFS [id: " + id + "]", ex);

					} catch (IOException ex) {
						LOGGER.error("IOException while fetching file from IPFS [id: {}]", id, ex);
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
				LOGGER.error("Exception while fetching file from IPFS [hash: {}]", multihash, ex);
				throw new TechnicalException("Exception while fetching file from IPFS " + multihash, ex);
			}
		}
	}

}
