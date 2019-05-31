package org.openplacereviews.opendb.dto;

public class IpfsStatusDTO {

	private String objects;
	private String status;
	private String peerId;
	private String version;
	private String gateway;
	private String api;
	private String addresses;
	private String publicKey;

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
}
