package org.openplacereviews.opendb.ops;

import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.service.DBConsensusManager.DBStaleException;

import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class OpPrivateBlocksList {

	private final Deque<OpBlock> blocks = new ConcurrentLinkedDeque<OpBlock>();
	private final Deque<OpBlock> blockHeaders = new ConcurrentLinkedDeque<OpBlock>();
	private final Map<String, OpBlock> blocksInfo = new ConcurrentHashMap<>();
	private final BlockDbAccessInterface dbAccess;
	
	public OpPrivateBlocksList() {
		this.dbAccess = null;
	}
	
	public OpPrivateBlocksList(Collection<OpBlock> headers, int superBlockDepth, BlockDbAccessInterface dbAccess) {
		if (headers != null) {
			for (OpBlock b : headers) {
				OpBlock blockHeader = new OpBlock(b, false, true).makeImmutable();
				blocksInfo.put(blockHeader.getRawHash(), blockHeader);
				blockHeaders.addLast(blockHeader);
			}
			updateHeaders(superBlockDepth);
		}
		this.dbAccess = dbAccess;
	}

	public Deque<OpBlock> getAllBlocks() {
		if(dbAccess != null){
			return dbAccess.getAllBlocks(blockHeaders);
		}
		return blocks;
	}
	
	public Deque<OpBlock> getAllBlockHeaders() {
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
	
	public OpBlock getBlockHeaderByHash(String rawHash) {
		return blocksInfo.get(rawHash);
	}
	
	public OpBlock getFullBlockByHash(String rawHash) throws DBStaleException {
		OpBlock b = blocksInfo.get(rawHash);
		if (b == null) {
			return null;
		}
		if(dbAccess != null){
			return dbAccess.getBlockByHash(rawHash);
		}
		for(OpBlock bs : blocks){
			if(bs.getBlockId() == b.getBlockId()) {
				return bs;
			}
		}
		return b;
	}
	
	
	public String getSuperBlockHash() {
		if (blockHeaders.size() == 0) {
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
		if(dbAccess != null){
			throw new UnsupportedOperationException();
		}
		blocks.push(block);
		addBlockHeader(block, superBlockDepth);
	}

	private void addBlockHeader(OpBlock block, int superBlockDepth) {
		OpBlock blockHeader = new OpBlock(block, false, true).makeImmutable();
		blocksInfo.put(blockHeader.getRawHash(), blockHeader);
		blockHeaders.push(blockHeader);
		updateHeaders(superBlockDepth);
	}

	private void updateHeaders(int superBlockDepth) {
		String sb = getSuperBlockHash();
		for(OpBlock blHeader : blockHeaders) {
			blHeader.putCacheObject(OpBlock.F_SUPERBLOCK_HASH, sb);
		}
	}


	void copyAndMerge(OpPrivateBlocksList copy, OpPrivateBlocksList parent, int superBlockDepth) {
		if(dbAccess != null){
			throw new UnsupportedOperationException();
		}
		blocks.addAll(copy.blocks);
		blocks.addAll(parent.blocks);
		
		blockHeaders.addAll(copy.blockHeaders);
		blockHeaders.addAll(parent.blockHeaders);
		
		blocksInfo.putAll(copy.blocksInfo);
		blocksInfo.putAll(parent.blocksInfo);
		updateHeaders(superBlockDepth);
		
	}

	void clear() {
		if(dbAccess != null){
			throw new UnsupportedOperationException();
		}
		blocks.clear();
		blocksInfo.clear();
		blockHeaders.clear();
		
	}


	
}
