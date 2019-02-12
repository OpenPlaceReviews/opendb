package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.SerializedName;

public class OpBlock {

	
	@SerializedName("block_id")
	public int blockId = -1;
	
	@SerializedName("version")
	public int version = 0;
	
	@SerializedName("date")
	public long date;
	
	@SerializedName("previous_block_hash")
	public String previousBlockHash;
	
	@SerializedName("merkle_tree_hash")
	public String merkleTreeHash;
	
	@SerializedName("hash")
	public String hash;
	
	
	private List<OpDefinitionBean> operations = new ArrayList<OpDefinitionBean>();
	

	
	public List<OpDefinitionBean> getOperations() {
		return operations;
	}
}
