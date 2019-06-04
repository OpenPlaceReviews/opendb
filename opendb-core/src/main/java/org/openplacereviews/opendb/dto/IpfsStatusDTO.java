package org.openplacereviews.opendb.dto;

import java.math.BigDecimal;
import java.util.List;

public class IpfsStatusDTO {

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
