package org.openplacereviews.opendb.service.ipfs.pinning;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.openplacereviews.opendb.util.exception.ConnectionException;
import org.openplacereviews.opendb.util.exception.TechnicalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class IPFSClusterPinningService implements PinningService {

	protected static final Logger LOGGER = LoggerFactory.getLogger(IPFSClusterPinningService.class);

	private static final String BASE_URI = "%s://%s:%s/";
	private static final String DEFAULT_PROTOCOL = "http";
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 9094;
	private static final ObjectMapper mapper = new ObjectMapper();

	private final String protocol;
	private final String host;
	private final Integer port;

	private IPFSClusterPinningService(String host, Integer port, String protocol) {
		this.host = host;
		this.port = port;
		this.protocol = protocol;
	}

	public static IPFSClusterPinningService connect() {
		return connect(DEFAULT_HOST, DEFAULT_PORT);
	}

	public static IPFSClusterPinningService connect(String host, Integer port) {
		return connect(host, port, DEFAULT_PROTOCOL);
	}

	public static IPFSClusterPinningService connect(String host, Integer port, String protocol) {
		try {
			LOGGER.trace("call GET {}://{}:{}/id", protocol, host, port);
			HttpResponse<String> response = Unirest.get(String.format(BASE_URI + "/id", protocol, host, port)).asString();
			LOGGER.info("Connected to IPFS-Cluster [protocol: {}, host: {}, port: {}]: Info {}", protocol, host, port, response.getBody());

			return new IPFSClusterPinningService(host, port, protocol);

		} catch (UnirestException ex) {
			String msg = String.format("Error whilst connecting to IPFS-Cluster [host: %s, port: %s]", host, port);
			LOGGER.error(msg, ex);
			throw new ConnectionException(msg, ex);
		}
	}

	@Override
	public void pin(String cid) {
		LOGGER.debug("pin CID {} on IPFS-cluster", cid);

		LOGGER.trace("call POST {}://{}:{}/pins/{}", protocol, host, port, cid);
		Unirest.post(String.format(BASE_URI + "/pins/%s", protocol, host, port, cid));

		LOGGER.debug("CID {} pinned on IPFS-cluster", cid);
	}

	@Override
	public void unpin(String cid) {
		LOGGER.debug("unpin CID {} on IPFS-cluster", cid);

		LOGGER.trace("call DELETE {}://{}:{}/pins/{}", protocol, host, port, cid);
		Unirest.delete(String.format(BASE_URI + "/pins/%s", protocol, host, port, cid));

		LOGGER.debug("unpin {} pinned on IPFS-cluster", cid);
	}

	@Override
	public List<String> getTracked() {
		LOGGER.debug("get pinned files on IPFS-cluster");

		try {

			LOGGER.trace("GET GET {}://{}:{}/pins", protocol, host, port);
			HttpResponse<String> response = Unirest.get(String.format(BASE_URI + "/pins", protocol, host, port))
					.asString();
			LOGGER.debug("response: {}", response);
			TrackedResponse result = mapper.readValue(response.getBody(), TrackedResponse.class);

			LOGGER.debug("get pinned files on IPFS-cluster");
			return result.getPins();

		} catch (UnirestException | IOException ex) {
			LOGGER.error("Exception converting HTTP response to JSON", ex);
			throw new TechnicalException("Exception converting HTTP response to JSON", ex);
		}
	}
}
