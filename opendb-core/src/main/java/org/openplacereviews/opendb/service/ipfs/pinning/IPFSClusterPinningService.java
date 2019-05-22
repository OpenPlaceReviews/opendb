package org.openplacereviews.opendb.service.ipfs.pinning;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.util.exception.ConnectionException;
import org.openplacereviews.opendb.util.exception.TechnicalException;

import java.io.IOException;
import java.util.List;

public class IPFSClusterPinningService implements PinningService {

	protected static final Log LOGGER = LogFactory.getLog(IPFSClusterPinningService.class);

	public static final String BASE_URI = "%s://%s:%s/";
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
			LOGGER.info(String.format("call GET %s://%s:%d/api/v0/id", protocol, host, port));
			HttpResponse<String> response = Unirest.get(String.format(BASE_URI + "api/v0/id", protocol, host, port)).asString();

			LOGGER.info(String.format("Connected to IPFS-Cluster [protocol: %s, host: %s, port: %d]: Info %s", protocol, host, port, response.getBody()));
			return new IPFSClusterPinningService(host, port, protocol);

		} catch (UnirestException ex) {
			String msg = String.format("Error whilst connecting to IPFS-Cluster [host: %s, port: %s]", host, port);
			LOGGER.error(msg, ex);
			throw new ConnectionException(msg, ex);
		}
	}

	@Override
	public boolean pin(String cid) throws UnirestException {
		LOGGER.debug(String.format("pin CID %s on IPFS-cluster", cid));

		LOGGER.debug(String.format("call POST %s://%s:%d/api/v0/pin/add?arg=%s&progress=true", protocol, host, port, cid));
		HttpResponse<JsonNode> jsonResponse = Unirest.post(String.format(BASE_URI + "/api/v0/pin/add?arg=%s&progress=true", protocol, host, port, cid)).asJson();

		if (jsonResponse.getStatus() == 200) {
			LOGGER.debug(String.format("CID %s pinned on IPFS-cluster", cid));
			return true;
		}

		LOGGER.error(String.format("CID %s was not pinned on IPFS-cluster", cid));
		return false;
	}

	@Override
	public boolean unpin(String cid) throws UnirestException {
		LOGGER.debug(String.format("unpin CID %s on IPFS-cluster", cid));

		LOGGER.debug(String.format("call DELETE %s://%s:%d/api/v0/pin/rm?arg=%s", protocol, host, port, cid));
		HttpResponse<JsonNode> jsonResponse = Unirest.delete(String.format(BASE_URI + "api/v0/pin/rm?arg=%s", protocol, host, port, cid)).asJson();

		if (jsonResponse.getStatus() == 200) {
			LOGGER.debug(String.format("unpin %s pinned on IPFS-cluster", cid));
			return true;
		}

		LOGGER.error(String.format("%s was not pinned on IPFS-cluster", cid));
		return false;
	}

	@Override
	public List<String> getTracked() {
		LOGGER.debug("get pinned files on IPFS-cluster");

		try {

			LOGGER.trace(String.format("GET GET %s://%s:%d/api/v0/pin/ls", protocol, host, port));
			HttpResponse<String> response = Unirest.get(String.format(BASE_URI + "/api/v0/pin/ls", protocol, host, port))
					.asString();
			LOGGER.debug(String.format("response: %s", response));
			TrackedResponse result = mapper.readValue(response.getBody(), TrackedResponse.class);

			LOGGER.debug("get pinned files on IPFS-cluster");
			return result.getPins();

		} catch (UnirestException | IOException ex) {
			LOGGER.error("Exception converting HTTP response to JSON", ex);
			throw new TechnicalException("Exception converting HTTP response to JSON", ex);
		}
	}
}
