package org.openplacereviews.opendb.dto;

import org.apache.commons.io.FilenameUtils;
import org.springframework.web.multipart.MultipartFile;

import java.util.Date;

public class ResourceDTO {

	private String type = "#image";
	private String hash;
	private String extension;
	private String cid;
	private transient boolean active = false;
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

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
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
