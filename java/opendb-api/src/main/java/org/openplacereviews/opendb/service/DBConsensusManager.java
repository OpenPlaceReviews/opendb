package org.openplacereviews.opendb.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

import wiremock.org.eclipse.jetty.util.ConcurrentHashSet;

@Service
public class DBConsensusManager {
	protected static final Log LOGGER = LogFactory.getLog(DBConsensusManager.class);
	
	private static final String FIELD_NAME = "name";
	
	// check SimulateSuperblockCompactSequences to verify numbers
	private static final double COMPACT_COEF = 0.5;
	private static final int SUPERBLOCK_SIZE_LIMIT_DB = 5;
	protected static final int COMPACT_ITERATIONS = 3;
		
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	private LogOperationService logSystem;

	private Map<String, OpBlock> blocks = new ConcurrentHashMap<String, OpBlock>();
	private Map<String, OpBlock> orphanedBlocks = new ConcurrentHashMap<String, OpBlock>();
	private OpBlockChain mainSavedChain = null;
	
	//////////// SYSTEM TABLES DDL ////////////
	protected static final String DDL_CREATE_TABLE_BLOCKS = "create table blocks (hash bytea PRIMARY KEY, phash bytea, blockid int, superblock bytea, details jsonb)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_HASH = "create index blocks_hash_ind on blocks(hash)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_PHASH = "create index blocks_phash_ind on blocks(phash)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_SUPERBLOCK = "create index blocks_superblock_ind on blocks(superblock)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_BLOCKID = "create index blocks_blockid_ind on blocks(blockid)";
	
	
	protected static final String DDL_CREATE_TABLE_OPS = "create table operations (dbid serial not null, hash bytea PRIMARY KEY, blocks bytea[], details jsonb)";
	protected static final String DDL_CREATE_TABLE_OPS_INDEX_HASH = "create index operations_hash_ind on operations(hash)";

	
	// Query / insert values 
	// select encode(b::bytea, 'hex') from test where b like (E'\\x39')::bytea||'%';
	// insert into test(b) values (decode('39556d070fd95f54b554010207d42605a8d0adfbb3b8b8e134df7df0689d78ab', 'hex'));
	// UPDATE blocks SET superblocks = array_remove(superblocks, decode('39556d070fd95f54b554010207d42605a8d0adfbb3b8b8e134df7df0689d78ab', 'hex'));

	public static void main(String[] args) {
		System.out.println(DDL_CREATE_TABLE_BLOCKS + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_HASH + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_PHASH + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_SUPERBLOCK + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_BLOCKID + ';');
		
		System.out.println(DDL_CREATE_TABLE_OPS + ';');
		System.out.println(DDL_CREATE_TABLE_OPS_INDEX_HASH + ';');
	}
	
	
	public Map<String, OpBlock> getOrphanedBlocks() {
		return orphanedBlocks;
	}

	protected String getHexFromPgObject(PGobject o) {
		String s = o.getValue();
		if(s == null) {
			return "";
		}
		if(!s.startsWith("\\x")) {
			throw new UnsupportedOperationException();
		}
		return s.substring(2);
	}
	
	public String getSuperblockHash() {
		return mainSavedChain.getSuperBlockHash();
	}

	// mainchain could change
	public synchronized OpBlockChain init(MetadataDb metadataDB) {
		final OpBlockchainRules rules = new OpBlockchainRules(formatter, logSystem);
		LOGGER.info("... Loading block headers ...");
		mainSavedChain = loadBlockHeadersAndBuildMainChain(rules);
		
		LOGGER.info(String.format("+++ Loaded %d block headers +++", blocks.size()));
		
		List<OpBlock> topBlockInfo = selectTopBlockFromOrphanedBlocks(mainSavedChain);
		if(!topBlockInfo.isEmpty()) {
			LOGGER.info(String.format("### Selected main blockchain with '%s' and %d id. Orphaned blocks %d. ###", 
					topBlockInfo.get(0).getRawHash(), topBlockInfo.get(0).getBlockId(), orphanedBlocks.size()));
		} else {
			LOGGER.info(String.format("### Selected main blockchain with '%s' and %d id. Orphaned blocks %d. ###", 
					mainSavedChain.getLastBlockRawHash(), mainSavedChain.getLastBlockId(), orphanedBlocks.size()));
		}
		LOGGER.info("... Loading blocks from database ...");
		OpBlockChain topChain = loadBlocks(topBlockInfo, mainSavedChain);
		LOGGER.info(String.format("### Loaded %d blocks ###", topChain.getSuperblockSize()));
		
		OpBlockChain blcQueue = new OpBlockChain(topChain, rules);
		
		LOGGER.info("... Loading operation queue  ...");
		int[] ops = new int[1];
		jdbcTemplate.query("SELECT details from operations where blocks is null order by dbid asc ", new RowCallbackHandler(){

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				ops[0]++;
				OpOperation op = formatter.parseOperation(rs.getString(1));
				op.makeImmutable();
				blcQueue.addOperation(op);
			}
			
		});
		LOGGER.info(String.format("... Loaded operation %d into queue  ...", ops[0]));
		LOGGER.info(String.format("+++ Database blockchain initialized +++"));
		return blcQueue;
	}



	private OpBlockChain loadBlocks(List<OpBlock> topBlockInfo, final OpBlockChain newParent) {
		if(topBlockInfo.size() == 0) {
			return newParent;
		}
		OpBlockChain blc = new OpBlockChain(newParent, newParent.getRules());
		for(OpBlock b : topBlockInfo) {
			String blockHash = b.getRawHash();
			jdbcTemplate.query("SELECT details from blocks where hash = ? ",
					new Object[] { SecUtils.getHashBytes(blockHash) }, new RowCallbackHandler() {

						@Override
						public void processRow(ResultSet rs) throws SQLException {
							OpBlock rawBlock = formatter.parseBlock(rs.getString(1));
							rawBlock.makeImmutable();
							OpBlock replicateBlock = blc.replicateBlock(rawBlock);
							if (replicateBlock == null) {
								throw new IllegalStateException("Could not replicate block: "
										+ formatter.toJson(rawBlock));
							}
						}
					});
		}
		return blc;
	}

	private OpBlock createBlockInfo(String blockHash, String pblockHash, int blockid) {
		OpBlock parent = blocks.get(pblockHash);
		if(!OUtils.isEmpty(pblockHash) && parent == null) {
			LOGGER.error(String.format("Orphaned block '%s' without parent '%s'.", blockHash, pblockHash ));
			return null;
		}
		// TODO Load full block header !!!!!
		OpBlock block = new OpBlock();
		block.putStringValue(OpBlock.F_HASH, blockHash); // ! not raw hash
		block.putStringValue(OpBlock.F_PREV_BLOCK_HASH, pblockHash);
		block.putObjectValue(OpBlock.F_BLOCKID, blockid);
		block.makeImmutable();
		blocks.put(blockHash, block);
		return block;
	}
	
	protected BlockDbAccessInterface createDbAccess(String superblock, List<OpBlock> blockHeaders) {
		// TODO !!!! DB ACCESS OR LOAD EVERYTHING !!!
		return null;
	}

	private OpBlockChain loadBlockHeadersAndBuildMainChain(final OpBlockchainRules rules) {
		OpBlockChain[] res = new OpBlockChain[] { OpBlockChain.NULL };
		boolean[] lastSuccess = new boolean[] { false };
		jdbcTemplate.query("SELECT hash, phash, blockid, superblock from blocks order by blockId asc", new RowCallbackHandler() {
			
			List<OpBlock> blockHeaders = new LinkedList<OpBlock>();
			String psuperblock;
			
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				String blockHash = SecUtils.hexify(rs.getBytes(1));
				String pblockHash = SecUtils.hexify(rs.getBytes(2));
				String superblock = SecUtils.hexify(rs.getBytes(4));
				OpBlock blockHeader = createBlockInfo(blockHash, pblockHash, rs.getInt(3));
				if(OUtils.isEmpty(superblock)) {
					orphanedBlocks.put(blockHash, blockHeader);
				} else {
					lastSuccess[0] = false;
					blockHeaders.add(blockHeader);
					if(OUtils.equals(psuperblock, superblock)) {
						// add to current chain
						if(OUtils.equals(superblock, OpBlockchainRules.calculateSuperblockHash(blockHeaders.size(), blockHeader.getRawHash()))) { 
							OpBlockChain parent = res[0];
							res[0] = new OpBlockChain(parent, blockHeaders, createDbAccess(superblock, blockHeaders), rules);
							blockHeaders.clear();
							lastSuccess[0] = true;
						}
					} else {
						if (!OUtils.equals(pblockHash, res[0].getLastBlockRawHash())) {
							throw new IllegalStateException(
								String.format("Block '%s'. Illegal parent '%s' != '%s' for superblock '%s'", blockHash, pblockHash,  
										res[0].getLastBlockRawHash(), superblock));
						}
					}
				}
			}
		});
		return res[0];
	}


	private List<OpBlock> selectTopBlockFromOrphanedBlocks(OpBlockChain prev) {
		OpBlock topBlockInfo = null;
		for(OpBlock bi : orphanedBlocks.values()) {
			if(topBlockInfo == null || topBlockInfo.getBlockId() < bi.getBlockId() ||
					(topBlockInfo.getBlockId() == bi.getBlockId() && topBlockInfo.getRawHash().compareTo(bi.getRawHash()) > 0)){
				topBlockInfo = bi;
			}
		}
		LinkedList<OpBlock> blockList = new LinkedList<OpBlock>();
		if(topBlockInfo != null && topBlockInfo.getBlockId() > prev.getLastBlockId()) {
			OpBlock blockInfo = topBlockInfo;
			while(blockInfo != null) {
				if(OUtils.equals(blockInfo.getRawHash(), prev.getLastBlockRawHash())) {
					return blockList;
				}
				orphanedBlocks.remove(blockInfo.getRawHash());
				blockList.addFirst(blockInfo);
				topBlockInfo = this.blocks.get(blockInfo.getPrevRawHash());
			}
			throw new IllegalStateException(String.format("Top selected block '%s' is not connected to superblock '%s'", 
					topBlockInfo.getRawHash(), prev.getLastBlockRawHash()));
		}
		return blockList;
	}



	public void insertBlock(OpBlock opBlock) {
		PGobject pGobject = new PGobject();
		pGobject.setType("jsonb");
		try {
			pGobject.setValue(formatter.toJson(opBlock));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		byte[] blockHash = SecUtils.getHashBytes(opBlock.getFullHash());
		String rawHash = SecUtils.hexify(blockHash);
		byte[] prevBlockHash = SecUtils.getHashBytes(opBlock.getStringValue(OpBlock.F_PREV_BLOCK_HASH));
		String rawPrevBlockHash = SecUtils.hexify(prevBlockHash);
		jdbcTemplate.update("INSERT INTO blocks(hash, phash, blockid, details) VALUES (?, ?, ?, ?)" , 
				blockHash, prevBlockHash, opBlock.getBlockId(), pGobject);
		for(OpOperation o : opBlock.getOperations()) {
			jdbcTemplate.update("UPDATE operations set blocks = blocks || ? where hash = ?" , 
					blockHash, SecUtils.getHashBytes(o.getHash()));	
		}
		OpBlock bi = createBlockInfo(rawHash, rawPrevBlockHash, opBlock.getBlockId());
		if(bi != null) {
			orphanedBlocks.put(rawHash, bi);
		}
	}
	
	
	public synchronized OpBlockChain saveMainBlockchain(OpBlockChain blc) {
		// find and saved last not saved part of the chain
		OpBlockChain lastNotSaved = null;
		OpBlockChain beforeLast = null;
		while(!blc.isNullBlock() && !blc.isDbAccessed()) {
			beforeLast = lastNotSaved;
			lastNotSaved = blc;
			blc = blc.getParent();
		}
		if(lastNotSaved != null && lastNotSaved.getSuperblockSize() >= SUPERBLOCK_SIZE_LIMIT_DB) {
			OpBlockChain saved = saveSuperblock(lastNotSaved);
			if(beforeLast != null) {
				beforeLast.changeToEqualParent(saved);
			} else {
				return saved;
			}
		}
		return blc;
	}
	
	private OpBlockChain saveSuperblock(OpBlockChain blc) {
		String superBlockHash = blc.getSuperBlockHash();
		LOGGER.info(String.format("Save superblock %s ", superBlockHash));
		if(!blc.getParent().getSuperBlockHash().equals(getSuperblockHash(parent))) {
			throw new IllegalStateException(
					String.format("DB-blockchain hash '%s' != '%s' in-memory blockchain",
							blc.getParent().getSuperBlockHash(), getSuperblockHash(parent)));
		}
		byte[] shash = SecUtils.getHashBytes(superBlockHash);
		Iterator<OpBlock> it = blc.getSuperblockHeaders().iterator();
		while (it.hasNext()) {
			OpBlock o = it.next();
			byte[] blHash = SecUtils.getHashBytes(o.getFullHash());
			String blockRawHash = SecUtils.hexify(blHash);
			// assign parent hash only for last block
			// LOGGER.info(String.format("Update block %s to superblock %s ", o.getHash(), superBlockHash));
			jdbcTemplate.update("UPDATE blocks set superblock = ?where hash = ?",
							shash, blHash);
			sc.blocks.addFirst(blockRawHash);
			sc.blocksSet.add(blockRawHash);
		}
		return sc;
	}
	
	public synchronized OpBlockChain compact(int prevSize, OpBlockChain blc, boolean db) {
		if(blc == null || blc.isNullBlock() || blc.getParent().isNullBlock()) {
			return blc;
		}
		OpBlockChain compactedParent = null;
		if(blc.isDbAccessed() == blc.getParent().isDbAccessed()) {
			compactedParent = compact(blc.getSuperblockSize(), blc.getParent(), db);
			// only 1 compact at a time
			boolean compact = compactedParent == blc.getParent();
			compact = compact && ((double) blc.getSuperblockSize() + COMPACT_COEF * prevSize) > ((double)blc.getParent().getSuperblockSize()) ;
			if(compact) {
				// TODO rewrite simulation
				if(blc.isDbAccessed() && db) {
					LOGGER.info(String.format("Compact db superblock '%s' into  superblock '%s' ", blc.getParent().getSuperBlockHash(), blc.getSuperBlockHash()));
					// TODO db compact
					return blc;
				} else {
					LOGGER.info(String.format("Compact runtime superblock '%s' into  superblock '%s' ", blc.getParent().getSuperBlockHash(), blc.getSuperBlockHash()));
					blc = new OpBlockChain(blc,  blc.getParent(), blc.getRules());
					printBlockChain(blc);
					return blc;
				}
			}
		} else {
			// redirect compact to parent 
			compactedParent = compact(0, blc.getParent(), db);
		}
		if(blc.getParent() != compactedParent) {
			blc.changeToEqualParent(compactedParent);
		}
		return blc;
	}
	

	private void printBlockChain(OpBlockChain blc) {
		List<String> superBlocksChain = new ArrayList<String>();
		OpBlockChain p = blc;
		while(p != null) {
			String sh = p.getSuperBlockHash();
			if(sh.length() > 10) {
				superBlocksChain.add(sh.substring(0, 10));
			}
			p = p.getParent();
		}
		LOGGER.info(String.format("Runtime chain %s", superBlocksChain));
	}
		
		
	
	public void insertOperation(OpOperation op) {
		PGobject pGobject = new PGobject();
		pGobject.setType("jsonb");
		try {
			pGobject.setValue(formatter.opToJson(op));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		jdbcTemplate.update("INSERT INTO operations(hash, details) VALUES (?, ?)" , 
				SecUtils.getHashBytes(op.getHash()), pGobject);
	}
	

}
