package org.openplacereviews.opendb.service.ipfs.dto;

import java.util.List;

public class TrackedResponse {

	private List<String> pins;

	public List<String> getPins() {
		return pins;
	}

	public void setPins(List<String> pins) {
		this.pins = pins;
	}
}
