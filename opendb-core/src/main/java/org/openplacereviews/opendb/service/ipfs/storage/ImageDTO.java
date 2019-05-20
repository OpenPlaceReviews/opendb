package org.openplacereviews.opendb.service.ipfs.storage;

import org.apache.commons.io.FilenameUtils;
import org.springframework.web.multipart.MultipartFile;

import java.util.Date;

public class ImageDTO {

	private String hash;
	private String extension;
	private String cid;
	private boolean active = false;
	private Date added;

	private transient MultipartFile multipartFile;

	public static ImageDTO of(MultipartFile multipartFile) {
		ImageDTO imageDTO = new ImageDTO();
		imageDTO.extension = FilenameUtils.getExtension(multipartFile.getOriginalFilename());
		imageDTO.multipartFile = multipartFile;

		return imageDTO;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
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
