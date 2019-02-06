package org.opengeoreviews.opendb.ops;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.SerializedName;

public class OpBlock {

	
	@SerializedName("block_id")
	public int blockId = -1;
	
	@SerializedName("date")
	public String date;
	
	@SerializedName("previous_block_hash")
	public String previousBlockHash;
	
	@SerializedName("block_hash")
	public String blockHash;
	
	
	private List<OpDefinitionBean> operations = new ArrayList<OpDefinitionBean>();
	

	
	public List<OpDefinitionBean> getOperations() {
		return operations;
	}
}
