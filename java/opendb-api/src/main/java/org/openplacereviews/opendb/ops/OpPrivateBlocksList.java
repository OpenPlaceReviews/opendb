package org.openplacereviews.opendb.ops;

import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class OpPrivateBlocksList {

	private final Deque<OpBlock> blocks = new ConcurrentLinkedDeque<OpBlock>();
	private final Deque<OpBlock> blockHeaders = new ConcurrentLinkedDeque<OpBlock>();
	private final Map<String, OpBlock> blocksInfo = new ConcurrentHashMap<>();
	
	
	public Collection<OpBlock> getAllBlocks() {
		// this queries db
		return blocks;
	}
	
	public Collection<OpBlock> getAllBlockHeaders() {
		return blockHeaders;
	}
	
	public OpBlock getLastBlockHeader() {
		return blockHeaders.peekFirst();
	}
	
	public OpBlock getFirstBlockHeader() {
		return blockHeaders.peekLast();
	}
	
	public int size() {
		return blockHeaders.size();
	}
	
	public OpBlock getBlockHeader(String rawHash) {
		return blocksInfo.get(rawHash);
	}
	
	public String getSuperBlockHash() {
		if (blocks.size() == 0) {
			return "";
		}
		OpBlock l = getLastBlockHeader();
		OpBlock f = getFirstBlockHeader();
		if( l == null || f == null) {
			return "";
		}
		String hash = l == null ? "" : l.getRawHash();
		int sz = l.getBlockId() - f.getBlockId() + 1;
		return OpBlockchainRules.calculateSuperblockHash(sz, hash);
	}


	void addBlock(OpBlock block, int superBlockDepth) {
		OpBlock blockHeader = new OpBlock(block, false, true).makeImmutable();
		blocksInfo.put(blockHeader.getRawHash(), blockHeader);
		blockHeaders.push(blockHeader);
		blocks.push(block);
		String sb = getSuperBlockHash();
		for(OpBlock blHeader : blockHeaders) {
			blHeader.putCacheObject(OpBlock.F_SUPERBLOCK_HASH, sb);
			blHeader.putCacheObject(OpBlock.F_SUPERBLOCK_ID, superBlockDepth);
		}
	}


	void copyAndMerge(OpPrivateBlocksList copy, OpPrivateBlocksList parent) {
		blocks.addAll(copy.blocks);
		blocks.addAll(parent.blocks);
		
		blockHeaders.addAll(copy.blockHeaders);
		blockHeaders.addAll(parent.blockHeaders);
		
		blocksInfo.putAll(copy.blocksInfo);
		blocksInfo.putAll(parent.blocksInfo);
		
	}

	void clear() {
		blocks.clear();
		blocksInfo.clear();
		blockHeaders.clear();
		
	}


	
}
