package org.opengeoreviews.opendb.ops;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class OpBlock {

	
	@SerializedName("block_id")
	private int blockId = -1;
	
	
	@SerializedName("date")
	private String date;
	
	
	private List<OpDefinitionBean> operations;
	
	
	public String getDate() {
		return date;
	}
	
	public int getBlockId() {
		return blockId;
	}
	
	
	public List<OpDefinitionBean> getOperations() {
		return operations;
	}
}
