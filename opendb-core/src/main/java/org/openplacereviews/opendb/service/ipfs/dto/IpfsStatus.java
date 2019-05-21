package org.openplacereviews.opendb.service.ipfs.dto;

public class IpfsStatus {

	private String status;
	private String peerId;
	private String version;
	private String gateway;
	private String api;
	private String addresses;
	private String publicKey;

	public String getStatus() {
		return status;
	}

	public IpfsStatus setStatus(String status) {
		this.status = status;
		return this;
	}

	public String getPeerId() {
		return peerId;
	}

	public IpfsStatus setPeerId(String peerId) {
		this.peerId = peerId;
		return this;
	}

	public String getVersion() {
		return version;
	}

	public IpfsStatus setVersion(String version) {
		this.version = version;
		return this;
	}

	public String getGateway() {
		return gateway;
	}

	public IpfsStatus setGateway(String gateway) {
		this.gateway = gateway;
		return this;
	}

	public String getApi() {
		return api;
	}

	public IpfsStatus setApi(String api) {
		this.api = api;
		return this;
	}

	public String getAddresses() {
		return addresses;
	}

	public IpfsStatus setAddresses(String addresses) {
		this.addresses = addresses;
		return this;
	}

	public String getPublicKey() {
		return publicKey;
	}

	public IpfsStatus setPublicKey(String publicKey) {
		this.publicKey = publicKey;
		return this;
	}
}
