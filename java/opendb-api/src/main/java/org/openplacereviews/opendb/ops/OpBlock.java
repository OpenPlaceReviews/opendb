package org.openplacereviews.opendb.ops;

import java.util.ArrayList;
import java.util.List;

public class OpBlock extends OpObject {
	
	public static final String F_HASH = "hash";
	public static final String F_BLOCKID = "block_id";
	public static final String F_VERSION = "version";
	public static final String F_DATE = "date";
	public static final String F_EXTRA = "extra"; // long
	public static final String F_DETAILS = "details"; // string
	public static final String F_SIGNED_BY = "signed_by";
	public static final String F_SIGNATURE = "signature";
	public static final String F_PREV_BLOCK_HASH = "previous_block_hash";
	public static final String F_MERKLE_TREE_HASH = "merkle_tree_hash";
	public static final String F_SIG_MERKLE_TREE_HASH = "sig_merkle_tree_hash";
	
	protected List<OpOperation> operations = new ArrayList<OpOperation>();
	
	public OpBlock() {
	}
	
	public OpBlock(OpBlock cp) {
		super(cp);
	}
	
	public List<OpOperation> getOperations() {
		return operations;
	}

	public int getBlockId() {
		return super.getIntValue(F_BLOCKID, -1);
	}

	public String getDateString() {
		return getStringValue(F_DATE);
	}
	
	public String getHash() {
		return getStringValue(F_HASH);
	}
	
	public String getSignature() {
		return getStringValue(F_SIGNATURE);
	}
	
}
