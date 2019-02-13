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
	
	@SerializedName("sig_merkle_tree_hash")
	public String sigMerkleTreeHash;
	
	@SerializedName("extra")
	public long extra;
	
	@SerializedName("details")
	public String details;
	
	@SerializedName("hash")
	public String hash;
	
	@SerializedName("signed_by")
	public String signedBy;
	
	@SerializedName("signature")
	public String signature;
	
	@SerializedName("signature_algo")
	public String signatureAlgo;
	
	
	public OpBlock() {
	}
	
	public OpBlock(OpBlock cp) {
		this.blockId = cp.blockId;
		this.version = cp.version;
		this.date = cp.date;
		this.previousBlockHash = cp.previousBlockHash;
		this.merkleTreeHash = cp.merkleTreeHash;
		this.sigMerkleTreeHash = cp.sigMerkleTreeHash;
		this.extra = cp.extra;
		this.details = cp.details;
		this.hash = cp.hash;
		this.signedBy = cp.signedBy;
		this.signature = cp.signature;
		this.signatureAlgo = cp.signatureAlgo;
		
	}
	
	private List<OpDefinitionBean> operations = new ArrayList<OpDefinitionBean>();
	

	
	public List<OpDefinitionBean> getOperations() {
		return operations;
	}
}
