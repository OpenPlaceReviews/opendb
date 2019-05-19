package org.openplacereviews.opendb.service.ipfs.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

//TODO change configuration!
@Configuration
public class IPFSSettings {

	@Value("${ipfs.host}")
	public static final String DEFAULT_HOST = "localhost";

	@Value("${ipfs.port}")
	public static final int DEFAULT_PORT = 5001;

	@Value("${ipfs.timeout}")
	public static final int DEFAULT_TIMEOUT = 10000;

	private String host = DEFAULT_HOST;
	private Integer port = DEFAULT_PORT;
	private String multiaddress;
	private int timeout = DEFAULT_TIMEOUT;

	public static IPFSSettings of(String host, Integer port, String multiaddress) {
		IPFSSettings s = new IPFSSettings();
		s.setHost(host);
		s.setPort(port);
		s.setMultiaddress(multiaddress);
		return s;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getMultiaddress() {
		return multiaddress;
	}

	public void setMultiaddress(String multiaddress) {
		this.multiaddress = multiaddress;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
}
