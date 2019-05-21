package org.openplacereviews.opendb.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.ops.de.OperationDeleteInfo;
import org.openplacereviews.opendb.service.ipfs.storage.ImageDTO;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.openplacereviews.opendb.service.DBSchemaManager.*;

@Service
public class DBConsensusManager {

	protected static final Log LOGGER = LogFactory.getLog(DBConsensusManager.class);
	
	// check SimulateSuperblockCompactSequences to verify numbers
	@Value("${opendb.db.compactCoefficient}")
	private double compactCoefficient = 1;
	
	@Value("${opendb.db.dbSuperblockSize}")
	private int superblockSize = 32;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private DBSchemaManager dbSchema;
	
	@Autowired
	private FileBackupManager backupManager;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	private LogOperationService logSystem;

	private Map<String, OpBlock> blocks = new ConcurrentHashMap<String, OpBlock>();
	private Map<String, OpBlock> orphanedBlocks = new ConcurrentHashMap<String, OpBlock>();
	private Map<String, SuperblockDbAccess> dbSuperBlocks = new ConcurrentHashMap<>();
	private OpBlockChain dbManagedChain = null;
	
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
		return dbManagedChain.getSuperBlockHash();
	}

	// mainchain could change
	public synchronized OpBlockChain init(MetadataDb metadataDB) {
		dbSchema.initializeDatabaseSchema(metadataDB, jdbcTemplate);
		backupManager.init();
		final OpBlockchainRules rules = new OpBlockchainRules(formatter, logSystem);
		LOGGER.info("... Loading block headers ...");
		dbManagedChain = loadBlockHeadersAndBuildMainChain(rules);
		
		LOGGER.info(String.format("+++ Loaded %d block headers +++", blocks.size()));
		
		LinkedList<OpBlock> topBlockInfo = selectTopBlockFromOrphanedBlocks(dbManagedChain);
//		LinkedList<OpBlock> topBlockInfo = new LinkedList<OpBlock>();
		if(!topBlockInfo.isEmpty()) {
			LOGGER.info(String.format("### Selected main blockchain with '%s' and %d id. Orphaned blocks %d. ###", 
					topBlockInfo.getLast().getRawHash(), topBlockInfo.getLast().getBlockId(), orphanedBlocks.size()));
		} else {
			LOGGER.info(String.format("### Selected main blockchain with '%s' and %d id. Orphaned blocks %d. ###", 
					dbManagedChain.getLastBlockRawHash(), dbManagedChain.getLastBlockId(), orphanedBlocks.size()));
		}
		LOGGER.info("... Loading blocks from database ...");
		OpBlockChain topChain = loadBlocks(topBlockInfo, dbManagedChain, rules);
		LOGGER.info(String.format("### Loaded %d blocks ###", topChain.getSuperblockSize()));
		
		OpBlockChain blcQueue = new OpBlockChain(topChain, rules);
		
		LOGGER.info("... Loading operation queue  ...");
		int[] ops = new int[1];
		jdbcTemplate.query("SELECT content from " + OPERATIONS_TABLE + " where blocks is null order by dbid asc ",
				new RowCallbackHandler() {

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

	


	private OpBlockChain loadBlocks(List<OpBlock> topBlockInfo, final OpBlockChain newParent, 
			final OpBlockchainRules rules) {
		if(topBlockInfo.size() == 0) {
			return newParent;
		}
		OpBlockChain blc = new OpBlockChain(newParent, rules);
		for (OpBlock b : topBlockInfo) {
			String blockHash = b.getRawHash();
			OpBlock rawBlock = loadBlock(blockHash);
			OpBlock replicateBlock = blc.replicateBlock(rawBlock);
			if (replicateBlock == null) {
				throw new IllegalStateException("Could not replicate block " + blockHash + " "
						+ formatter.toJson(rawBlock));
			}
		}
		return blc;
	}


	private OpBlock loadBlock(String blockHash) {
		List<OpBlock> blocks = jdbcTemplate.query("SELECT content from " + BLOCKS_TABLE + " where hash = ? ",
				new Object[] { SecUtils.getHashBytes(blockHash) }, new RowMapper<OpBlock>() {

					@Override
					public OpBlock mapRow(ResultSet rs, int rowNum) throws SQLException {
						OpBlock rawBlock = formatter.parseBlock(rs.getString(1));
						rawBlock.makeImmutable();
						return rawBlock;
					}

				});
		if(blocks.size() > 1) {
			throw new UnsupportedOperationException("Duplicated blocks for the same hash: " + blockHash);
		}
		OpBlock rawBlock = blocks.size() == 0 ? null : blocks.get(0);
		return rawBlock;
	}

	private OpBlockChain compactTwoDBAccessed(OpBlockChain blc) {
		LOGGER.info(String.format("Compacting db superblock '%s' into  superblock '%s'", 
				blc.getParent().getSuperBlockHash(), blc.getSuperBlockHash()));
		SuperblockDbAccess dbSB = dbSuperBlocks.get(blc.getSuperBlockHash());
		SuperblockDbAccess dbPSB = dbSuperBlocks.get(blc.getParent().getSuperBlockHash());
		OpBlockChain res = blc;
		boolean txRollback = false;
		try {
			dbSB.markAsStale(true);
			dbPSB.markAsStale(true);
			List<OpBlock> blockHeaders = new ArrayList<OpBlock>();
			blockHeaders.addAll(blc.getSuperblockHeaders());
			blockHeaders.addAll(blc.getParent().getSuperblockHeaders());
			String newSuperblockHash = OpBlockchainRules.calculateSuperblockHash(blockHeaders.size(), blc.getLastBlockRawHash());
			byte[] sbHashCurrent = SecUtils.getHashBytes(blc.getSuperBlockHash());
			byte[] sbHashParent = SecUtils.getHashBytes(blc.getParent().getSuperBlockHash());
			byte[] sbHashNew = SecUtils.getHashBytes(newSuperblockHash);
			
			// Connection conn = dataSource.getConnection();
			jdbcTemplate.execute("BEGIN");
			txRollback = true;
			jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashCurrent);
			jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashParent);
			
			jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashCurrent);
			jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashParent);
			
			String query = "INSERT into " + OP_DELETED_TABLE + "(hash, superblock, shash, mask) "
					+" SELECT coalesce(r1.hash, r2.hash), ?, r1.shash || r2.shash, r1.mask | r2.mask FROM " 
					+ " (select * from "+ OP_DELETED_TABLE +" where superblock = ? ) r1 "
					+ "  FULL OUTER JOIN " 
					+ " (select * from "+ OP_DELETED_TABLE +" where superblock = ? ) r2 "
					+ " ON r1.hash = r2.hash";
			jdbcTemplate.update(query, sbHashNew, sbHashCurrent, sbHashParent);
			jdbcTemplate.update("DELETE FROM " + OP_DELETED_TABLE + " WHERE superblock = ? ", sbHashParent);
			jdbcTemplate.update("DELETE FROM " + OP_DELETED_TABLE + " WHERE superblock = ? ", sbHashCurrent);
			
			for(String objTable : dbSchema.getObjectTables()) {
				String queryIns = "INSERT INTO " + objTable + "(type, "+ dbSchema.generatePKString(objTable, "p%1$d", ", ") + ", ophash, superblock, sblockid, sorder, content) "
					+ " SELECT coalesce(r1.type, r2.type), " + dbSchema.generatePKString(objTable, "coalesce(r1.p%1$d, r2.p%1$d)", ", ") + ", " 
					+ "     coalesce(r1.ophash, r2.ophash), ?, coalesce(r1.sblockid, r2.sblockid), coalesce(r1.sorder, r2.sorder), coalesce(r1.content, r2.content) " 
					+ " FROM "
					+ " (select * from " + objTable +" where superblock = ? ) r1 "
					+ "  FULL OUTER JOIN " 
					+ " (select * from " + objTable +" where superblock = ? ) r2 "
					+ " ON r1.type = r2.type and " + dbSchema.generatePKString(objTable, "r1.p%1$d is not distinct from r2.p%1$d", " and ");
				jdbcTemplate.update(queryIns, sbHashNew, sbHashCurrent, sbHashParent);
				jdbcTemplate.update("DELETE FROM " + objTable + " WHERE superblock = ? ", sbHashParent);
				jdbcTemplate.update("DELETE FROM " + objTable + " WHERE superblock = ? ", sbHashCurrent);
			}

			res = new OpBlockChain(blc.getParent().getParent(), 
					blockHeaders, createDbAccess(newSuperblockHash, blockHeaders), blc.getRules());
			jdbcTemplate.execute("COMMIT");
			txRollback = true;
		} finally {
			if (txRollback) {
				try {
					jdbcTemplate.execute("ROLLBACK");
				} catch (DataAccessException e) {
					LOGGER.error(String.format("Error while rollback %s ", e.getMessage()), e);
				}
				// revert
				dbSB.markAsStale(false);
				dbPSB.markAsStale(false);
			}
		}
		return res;
	}
	
	protected class SuperblockDbAccess implements BlockDbAccessInterface {

		protected final String superBlockHash;
		protected final List<OpBlock> blockHeaders;
		private final ReentrantReadWriteLock readWriteLock;
		private final ReadLock readLock;
		private volatile boolean staleAccess;
		private final byte[] sbhash;
		
		public SuperblockDbAccess(String superBlockHash, Collection<OpBlock> blockHeaders) {
			this.superBlockHash = superBlockHash;
			sbhash = SecUtils.getHashBytes(superBlockHash);
			this.blockHeaders = new ArrayList<OpBlock>(blockHeaders);
			this.readWriteLock = new ReentrantReadWriteLock();
			readLock = this.readWriteLock.readLock();
			dbSuperBlocks.put(superBlockHash, this);
		}
		
		public boolean markAsStale(boolean stale) {
			WriteLock lock = readWriteLock.writeLock();
			lock.lock();
			try {
				staleAccess = stale;
				return true;
			} finally {
				lock.unlock();
			}
		}

		@Override
		public OpObject getObjectById(String type, CompoundKey k) {
			readLock.lock();
			try {
				if(staleAccess) {
					throw new UnsupportedOperationException();
				}
				int sz = k.size();
				Object[] o = new Object[sz + 2];
				o[0] = sbhash;
				o[1] = type;
				k.toArray(o, 2);
				String table = dbSchema.getTableByType(type);
				if (sz > dbSchema.getKeySizeByType(type)) {
					throw new UnsupportedOperationException();
				}
				String s = "select content, type, ophash from " + table +  
							" where superblock = ? and type = ? and " +
						dbSchema.generatePKString(table, "p%1$d = ?", " and ", sz);
				return jdbcTemplate.query(s, o, new ResultSetExtractor<OpObject>() {

					@Override
					public OpObject extractData(ResultSet rs) throws SQLException, DataAccessException {
						if(!rs.next()) {
							return null;
						}
						OpObject obj = formatter.parseObject(rs.getString(1));
						obj.setParentOp(rs.getString(2), SecUtils.hexify(rs.getBytes(3)));
						return obj;
					}
				});
			} finally {
				readLock.unlock();
			}
		}


		@Override
		public Map<CompoundKey, OpObject> getAllObjects(String type, ObjectsSearchRequest request) {
			int limit = request.limit - request.result.size();
			if(limit <= 0 && request.limit >= 0) {
				return Collections.emptyMap();
			}
			readLock.lock();
			try {
				if(staleAccess) {
					throw new UnsupportedOperationException();
				}
				Object[] o = new Object[2];
				o[0] = sbhash;
				o[1] = type;
				
				String objTable = dbSchema.getTableByType(type);
				final int keySize = dbSchema.getKeySizeByType(type);
				String sql = "select content, type, ophash, " + dbSchema.generatePKString(objTable, "p%1$d", ", ")
						+ "  from " + objTable + " where superblock = ? and type = ? ";
				if(limit > 0) {
					sql = sql + " limit " + limit;
				}
				Map<CompoundKey, OpObject> res = new LinkedHashMap<CompoundKey, OpObject>();
				jdbcTemplate.query(sql, o, new RowCallbackHandler() {
					List<String> ls = new ArrayList<String>(5);
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						ls.clear();
						for(int i = 0; i < keySize; i++) {
							ls.add(rs.getString(i + 4));
						}
						CompoundKey k = new CompoundKey(0, ls);
						OpObject obj = formatter.parseObject(rs.getString(1));
						obj.setParentOp(rs.getString(2), SecUtils.hexify(rs.getBytes(3)));
						res.put(k, obj);
					}
				});
				return res;
			} finally {
				readLock.unlock();
			}
		}

		@Override
		public OperationDeleteInfo getOperationInfo(String rawHash) {
			readLock.lock();
			try {
				if(staleAccess) {
					throw new UnsupportedOperationException();
				}
				Object[] o = new Object[2];
				o[0] = sbhash;
				o[1] = SecUtils.getHashBytes(rawHash);
				String sql = "select d.mask, d.shash, o.content from " + OP_DELETED_TABLE + " d join " + OPERATIONS_TABLE + " o on o.hash = d.hash "
						+ " where d.superblock = ? and d.hash = ? ";
				final OperationDeleteInfo od = new OperationDeleteInfo();
				jdbcTemplate.query(sql, o, new RowCallbackHandler(){

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						long bigInt = rs.getLong(1);
						od.create = bigInt % 2 == 0;
						BitSet bs = BitSet.valueOf(new long[]{bigInt >> 1});
						Array ar = rs.getArray(2);
						PGobject[] ls = (PGobject[]) (ar == null ? null : ar.getArray());
						if(ls != null) {
							od.deletedOpHashes = new ArrayList<String>();
							for(int k = 0; k < ls.length; k++) {
								System.out.println("TODO VALIDATION hash dex format !!! : " + ls[k].getValue());
								od.deletedOpHashes.add(ls[k].getValue());
							}
						}
						od.deletedObjects = new boolean[bs.length()];
						for(int i = 0; i < od.deletedObjects.length; i++) {
							od.deletedObjects[i] = bs.get(i);
						}
						od.op = formatter.parseOperation(rs.getString(3));
					}
				});
				if(od.op == null) {
					return null;
				}
				return od;
			} finally {
				readLock.unlock();
			}
		}

		@Override
		public Deque<OpBlock> getAllBlocks(Collection<OpBlock> blockHeaders) {
			boolean isSuperblockReferenceActive = false;
			readLock.lock();
			try {
				isSuperblockReferenceActive = !staleAccess;
			} finally {
				readLock.unlock();
			}
			if (isSuperblockReferenceActive) {
				// to do faster to load by superblock reference, so it could be 1 sql
			}
			LinkedList<OpBlock> blocks = new LinkedList<OpBlock>();
			for (OpBlock b : blockHeaders) {
				OpBlock lb = loadBlock(b.getRawHash());
				if (lb == null) {
					throw new IllegalStateException(String.format("Couldn't load '%s' block from db", b.getRawHash()));
				}
				blocks.add(lb);
			}
			return blocks;
		}

		@Override
		public OpBlock getBlockByHash(String rawHash) {
			return loadBlock(rawHash);
		}

		
	}
	
	protected BlockDbAccessInterface createDbAccess(String superblock, Collection<OpBlock> blockHeaders) {
		return new SuperblockDbAccess(superblock, blockHeaders);
	}

	private OpBlockChain loadBlockHeadersAndBuildMainChain(final OpBlockchainRules rules) {
		OpBlockChain[] res = new OpBlockChain[] { OpBlockChain.NULL };
		jdbcTemplate.query("SELECT hash, phash, blockid, superblock, header from " + BLOCKS_TABLE + " order by blockId asc", new RowCallbackHandler() {
			
			LinkedList<OpBlock> blockHeaders = new LinkedList<OpBlock>();
			
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				String blockHash = SecUtils.hexify(rs.getBytes(1));
				String pblockHash = SecUtils.hexify(rs.getBytes(2));
				String superblock = SecUtils.hexify(rs.getBytes(4));
				OpBlock parentBlockHeader = blocks.get(pblockHash);
				OpBlock blockHeader = formatter.parseBlock(rs.getString(5));
				blocks.put(blockHash, blockHeader);
				if(!OUtils.isEmpty(pblockHash) && parentBlockHeader == null) {
					LOGGER.error(String.format("Orphaned block '%s' without parent '%s'.", blockHash, pblockHash));
					orphanedBlocks.put(blockHash, blockHeader);
					return;
				} else if(OUtils.isEmpty(superblock)) {
					orphanedBlocks.put(blockHash, blockHeader);
				} else {
					String lastBlockHash = blockHeaders.size() == 0 ? res[0].getLastBlockRawHash() : blockHeaders.getFirst().getRawHash();
					blockHeaders.push(blockHeader);
					String currentSuperblock = OpBlockchainRules.calculateSuperblockHash(blockHeaders.size(), blockHeader.getRawHash());
					// add to current chain
					if (!OUtils.equals(pblockHash, lastBlockHash)) {
						throw new IllegalStateException(
							String.format("Block '%s'. Illegal parent '%s' != '%s' for superblock '%s'", blockHash, pblockHash,  
									res[0].getLastBlockRawHash(), superblock));
					}
					if (OUtils.equals(superblock, currentSuperblock)) {
						OpBlockChain parent = res[0];
						res[0] = new OpBlockChain(parent, blockHeaders, createDbAccess(superblock, blockHeaders), rules);
						blockHeaders.clear();
					}
				}
			}
		});
		return res[0];
	}


	private boolean isConnected(OpBlock bi, String lastBlockRawHash) {
		if(bi.getBlockId() == 0) {
			return lastBlockRawHash.length() == 0;
		}
		if(bi.getRawHash().equals(lastBlockRawHash)) {
			return true;
		}
		String prevRawHash = bi.getPrevRawHash();
		OpBlock parentBlock = blocks.get(prevRawHash);
		if(parentBlock != null) {
			return isConnected(parentBlock, lastBlockRawHash);
		}
		return false;
	}
	
	private LinkedList<OpBlock> selectTopBlockFromOrphanedBlocks(OpBlockChain prev) {
		OpBlock topBlockInfo = null;
		String lastBlockRawHash = prev.getLastBlockRawHash();
		for(OpBlock bi : orphanedBlocks.values()) {
			boolean isNewer = false;
			if(topBlockInfo == null) {
				isNewer = true;
			} else if(topBlockInfo.getBlockId() < bi.getBlockId()) {
				isNewer = true;
			} else if(topBlockInfo.getBlockId() == bi.getBlockId() && topBlockInfo.getRawHash().compareTo(bi.getRawHash()) > 0) {
				isNewer = true;
			}
			if(isNewer && isConnected(bi, lastBlockRawHash)) {
				topBlockInfo = bi;
			}
		}
		// returns in order from the oldest to the newest
		LinkedList<OpBlock> blockList = new LinkedList<OpBlock>();
		if(topBlockInfo != null && topBlockInfo.getBlockId() > prev.getLastBlockId()) {
			OpBlock blockInfo = topBlockInfo;
			LinkedList<String> blocksInfoLst = new LinkedList<String>();
			while(blockInfo != null) {
				if(OUtils.equals(blockInfo.getRawHash(), lastBlockRawHash)) {
					return blockList;
				}
				orphanedBlocks.remove(blockInfo.getRawHash());
				blockList.addFirst(blockInfo);
				blocksInfoLst.addFirst(blockInfo.getRawHash());
				blockInfo = blocks.get(blockInfo.getPrevRawHash());
			}
			if(OUtils.isEmpty(lastBlockRawHash)) {
				return blockList;
			}
			throw new IllegalStateException(String.format("Top selected block '%s' is not connected to superblock '%s'", 
					blocksInfoLst.toString(), prev.getLastBlockRawHash()));
		}
		return blockList;
	}



	public void insertBlock(OpBlock opBlock) {
		OpBlock blockheader = new OpBlock(opBlock, false, true);
		PGobject blockObj = new PGobject();
		blockObj.setType("jsonb");
		PGobject blockHeaderObj = new PGobject();
		blockHeaderObj.setType("jsonb");
		try {
			blockObj.setValue(formatter.toJson(opBlock));
			blockHeaderObj.setValue(formatter.fullObjectToJson(blockheader));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		
		
		byte[] blockHash = SecUtils.getHashBytes(opBlock.getFullHash());
		String rawHash = SecUtils.hexify(blockHash);
		byte[] prevBlockHash = SecUtils.getHashBytes(opBlock.getStringValue(OpBlock.F_PREV_BLOCK_HASH));
//		String rawPrevBlockHash = SecUtils.hexify(prevBlockHash);
		jdbcTemplate.execute("BEGIN");
		boolean succeed = false;
		try {
			jdbcTemplate.update("INSERT INTO " + BLOCKS_TABLE
					+ " (hash, phash, blockid, header, content ) VALUES (?, ?, ?, ?, ?)", blockHash, prevBlockHash,
					opBlock.getBlockId(), blockHeaderObj, blockObj);
			for (OpOperation o : opBlock.getOperations()) {
				int upd = jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set blocks = blocks || ? where hash = ?", blockHash,
						SecUtils.getHashBytes(o.getHash()));
				if (upd == 0) {
					throw new IllegalArgumentException(
							String.format("Can't create block '%s' cause op '%s' doesn't exist", opBlock.getRawHash(), o.getHash()));
				}
			}
			jdbcTemplate.execute("COMMIT");
			succeed = true;
		} finally {
			if (!succeed) {
				try {
					jdbcTemplate.execute("ROLLBACK");
				} catch (DataAccessException e) {
					LOGGER.error(String.format("Error while rollback %s ", e.getMessage()), e);
				}
			}
		}
		backupManager.insertBlock(opBlock);
		blocks.put(rawHash, blockheader);
		orphanedBlocks.put(rawHash, blockheader);
	}
	
	
	public synchronized OpBlockChain saveMainBlockchain(OpBlockChain blc) {
		// find and saved last not saved part of the chain
		OpBlockChain lastNotSaved = null;
		OpBlockChain beforeLast = null;
		boolean parentInOrphanedList = true;
		while(!blc.isNullBlock() && !blc.isDbAccessed()) {
			beforeLast = lastNotSaved;
			lastNotSaved = blc;
			if(parentInOrphanedList) {
				for(OpBlock header : blc.getSuperblockHeaders()) {
					OpBlock existing = orphanedBlocks.remove(header.getRawHash());
					if(existing == null) {
						parentInOrphanedList = false;
						break;
					}
				}
			}
			blc = blc.getParent();
		}
		if(lastNotSaved != null && lastNotSaved.getSuperblockSize() >= superblockSize) {
			OpBlockChain saved = saveSuperblock(lastNotSaved);
			if(beforeLast != null) {
				if(!beforeLast.changeToEqualParent(saved)) {
					throw new IllegalStateException(
							"Can't change parent " + lastNotSaved.getSuperBlockHash() + " " + saved.getSuperBlockHash());
				}
			} else {
				return saved;
			}
		}
		return blc;
	}
	
	
	private OpBlockChain saveSuperblock(OpBlockChain blc) {
		blc.validateLocked();
		String superBlockHashStr = blc.getSuperBlockHash();
		LOGGER.info(String.format("Save superblock %s ", superBlockHashStr));
		byte[] superBlockHash = SecUtils.getHashBytes(blc.getSuperBlockHash());
		Collection<OpBlock> blockHeaders = blc.getSuperblockHeaders();
		OpBlockChain dbchain = null;
		
		jdbcTemplate.execute("BEGIN");
		try {
			Map<String, Long> opsId = new HashMap<String, Long>(); 
			for(OpBlock block : blc.getSuperblockFullBlocks()) {
				byte[] blHash = SecUtils.getHashBytes(block.getFullHash());
				// assign parent hash only for last block
				// String blockRawHash = SecUtils.hexify(blHash);
				// LOGGER.info(String.format("Update block %s to superblock %s ", o.getHash(), superBlockHash));
				jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = ? where hash = ?", superBlockHash, blHash);
				int order = 0;
				int bid = block.getBlockId();
				for(OpOperation op : block.getOperations()) {
					long l = OUtils.combine(bid, order);
					opsId.put(op.getRawHash(), l);
					jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = ?, sblockid = ?, sorder = ? where hash = ?", 
							superBlockHash, bid, order, SecUtils.getHashBytes(op.getRawHash()));
					order ++;
				}
			}
			Collection<OperationDeleteInfo> delInfo = blc.getSuperblockDeleteInfo();
			for(OperationDeleteInfo oi : delInfo) {
				byte[] opHash = SecUtils.getHashBytes(oi.op.getRawHash());
				BitSet bs = new BitSet();
				if(oi.create) {
					bs.set(0);
				}
				if(oi.deletedObjects != null) {
					if(oi.deletedObjects.length > 62) {
						throw new UnsupportedOperationException(String.format("Deleting %d objects is not supported", oi.deletedObjects.length));
					}
					for(int i = 0; i < oi.deletedObjects.length; i++) {
						if(oi.deletedObjects[i]){
							bs.set(i + 1);
						}
					}
				}
				final long[] ls = bs.toLongArray();
				final String[] sobjs = new String[oi.deletedOpHashes == null ? 0 : oi.deletedOpHashes.size()];
				for (int i = 0; i < sobjs.length; i++) {
					// sobjs[i] = SecUtils.getHashBytes(oi.deletedOpHashes.get(i));
					sobjs[i] = "\\x" + oi.deletedOpHashes.get(i);
				}
				jdbcTemplate.update(new PreparedStatementCreator() {
					
					@Override
					public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
						Array refs = con.createArrayOf("bytea", sobjs);
						PreparedStatement pt = con.prepareStatement("INSERT INTO " + OP_DELETED_TABLE + "(hash,superblock,shash,mask)"
											+ " VALUES(?,?,?,?)");
						pt.setBytes(1, opHash);
						pt.setBytes(2, superBlockHash);
						pt.setArray(3, refs);
						pt.setLong(4, ls[0]);
						return pt;
					}
				});
			}
			
			Map<String, Map<CompoundKey, OpObject>> so = blc.getSuperblockObjects();
			for (String type : so.keySet()) {
				Map<CompoundKey, OpObject> objects = so.get(type);
				Iterator<Entry<CompoundKey, OpObject>> it = objects.entrySet().iterator();
				while (it.hasNext()) {
					Entry<CompoundKey, OpObject> e = it.next();
					CompoundKey pkey = e.getKey();
					OpObject obj = e.getValue();
					long l = opsId.get(obj.getParentHash());
					int sblockid = OUtils.first(l);
					int sorder = OUtils.second(l);
					dbSchema.insertObjIntoTable(type, pkey, obj, superBlockHash, sblockid, sorder, jdbcTemplate);
				}
			}
			dbchain = new OpBlockChain(blc.getParent(), blockHeaders, createDbAccess(superBlockHashStr, blockHeaders),
					blc.getRules());
			jdbcTemplate.execute("COMMIT");
		} finally {
			if (dbchain == null) {
				try {
					jdbcTemplate.execute("ROLLBACK");
				} catch (DataAccessException e) {
					LOGGER.error(String.format("Error while rollback %s ", e.getMessage()), e);
				}
			}
		}
		return dbchain;
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
			compact = compact && ((double) blc.getSuperblockSize() + compactCoefficient * prevSize) > ((double)blc.getParent().getSuperblockSize()) ;
			if(compact) {
				LOGGER.info("Chain to compact: ");
				printBlockChain(blc);
				// See @SimulateSuperblockCompactSequences
				if(blc.isDbAccessed() && db) {
					// here we need to lock all db access of 2 blocks and run update in 1 transaction
					return compactTwoDBAccessed(blc);
				} else {
					LOGGER.info(String.format("Compact runtime superblock '%s' into  superblock '%s' ", blc.getParent().getSuperBlockHash(), blc.getSuperBlockHash()));
					blc = new OpBlockChain(blc,  blc.getParent(), blc.getRules());
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
		while(p != null && !p.isNullBlock()) {
			String sh = p.getSuperBlockHash();
			if (sh.startsWith("00")) {
				while (sh.startsWith("00")) {
					sh = sh.substring(2);
				}
				sh = "0*" + sh;
			}
			if(sh.length() > 10) {
				sh = sh.substring(0, 10);
			}
			if(p.isDbAccessed()) {
				sh = "db-"+sh;
			}
			superBlocksChain.add(sh);
			p = p.getParent();
		}
		LOGGER.info(String.format("Runtime chain %s", superBlocksChain));
	}
		
		
	
	public int removeOperations(Set<String> ops) {
		int deleted = 0;
		// simple approach without using transaction isolations
		for (String op : ops) {
			deleted += jdbcTemplate.update("WITH moved_rows AS ( DELETE FROM " + OPERATIONS_TABLE
					+ "     a WHERE hash = ? and (blocks = '{}' or blocks is null) RETURNING a.*) " + " INSERT INTO "
					+ OPERATIONS_TRASH_TABLE
					+ " (id, hash, time, content) SELECT dbid, hash, now(), content FROM moved_rows",
					SecUtils.getHashBytes(op));
		}
		return deleted;
	}
	
	public boolean removeFullBlock(OpBlock block) {
		boolean txRollback = false;
		try {
			jdbcTemplate.execute("BEGIN");
			txRollback = true;
			byte[] blockHash = SecUtils.getHashBytes(block.getRawHash());
			int upd = jdbcTemplate.update("WITH moved_rows AS ( DELETE FROM " + BLOCKS_TABLE + " a WHERE hash = ? and superblock is null RETURNING a.*) "
					+ " INSERT INTO " + BLOCKS_TRASH_TABLE
					+ " (hash, phash, blockid, time, content) SELECT hash, phash, blockid, now(), content FROM moved_rows", blockHash);
			if(upd != 0) {
				// to do: 
				// here we need to decide what to do with operations with empty blocks[] (they will be added to the queue after restart otherwise)
				for (OpOperation o : block.getOperations()) {
					jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE
							+ " set blocks = array_remove(blocks, ?) where hash = ?", 
							blockHash, SecUtils.getHashBytes(o.getRawHash()));
				}
				orphanedBlocks.remove(block.getRawHash());
				blocks.remove(block.getRawHash());
			}
			jdbcTemplate.execute("COMMIT");
			txRollback = false;
			return upd != 0;
		} finally {
			if (txRollback) {
				try {
					jdbcTemplate.execute("ROLLBACK");
				} catch (DataAccessException e) {
					LOGGER.error(String.format("Error while rollback %s ", e.getMessage()), e);
				}
			}
		}
	}
	
	public OpBlockChain unloadSuperblockFromDB(OpBlockChain blc) {
		if(blc.isDbAccessed()) {
			SuperblockDbAccess dba = dbSuperBlocks.get(blc.getSuperBlockHash());
			OpBlockChain res = new OpBlockChain(blc.getParent(), blc.getRules());
			List<OpBlock> lst = new ArrayList<OpBlock>(blc.getSuperblockFullBlocks());
			byte[] blockHash = SecUtils.getHashBytes(blc.getSuperBlockHash());
			Collections.reverse(lst);
			for(OpBlock block : lst) {
				res.replicateBlock(block);
			}
			boolean txRollback = false;
			try {
				dba.markAsStale(true);
				jdbcTemplate.execute("BEGIN");
				txRollback = true;
				jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = NULL where superblock = ?", blockHash);
				jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = NULL where superblock = ? ", blockHash);
				jdbcTemplate.update("DELETE FROM " + OP_DELETED_TABLE + " where superblock = ?", blockHash);
				for (String objTable : dbSchema.getObjectTables()) {
					jdbcTemplate.update("DELETE FROM " + objTable + " where superblock = ?", blockHash);
				}
				
				jdbcTemplate.execute("COMMIT");
				txRollback = false;
				return res;
			} finally {
				if (txRollback) {
					try {
						jdbcTemplate.execute("ROLLBACK");
					} catch (DataAccessException e) {
						LOGGER.error(String.format("Error while rollback %s ", e.getMessage()), e);
					}
				}
				// revert
				dba.markAsStale(false);
			}
		}
		return blc;
	}

	public ImageDTO storeImageObject(ImageDTO imageDTO) throws IOException {
		imageDTO.setHash(SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, imageDTO.getMultipartFile().getBytes()));

		if (imageObjectIsExist(imageDTO) == null) {
			imageDTO.setAdded(new Date());
			jdbcTemplate.update("INSERT INTO " + IMAGE_TABLE + "(hash, extension, cid, active, added) VALUES (?, ?, ?, ?, ?)",
					imageDTO.getHash(), imageDTO.getExtension(), imageDTO.getCid(), imageDTO.isActive(), imageDTO.getAdded());
		}

		return imageDTO;
	}

	public ImageDTO imageObjectIsExist(ImageDTO imageDTO) {
		return jdbcTemplate.query("select added, active from " + IMAGE_TABLE + " where hash = ?", new ResultSetExtractor<ImageDTO>() {

			@Override
			public ImageDTO extractData(ResultSet rs) throws SQLException, DataAccessException {
				if (rs.next()) {
					imageDTO.setAdded(rs.getTimestamp(1));
					imageDTO.setActive(rs.getBoolean(2));
					return imageDTO;
				}
				return null;
			}
		}, imageDTO.getHash());
	}

	public ImageDTO loadImageObjectIfExist(String cid) {
		return jdbcTemplate.query("select hash, extension, cid, active, added from " + IMAGE_TABLE + " where cid = ?", new ResultSetExtractor<ImageDTO>() {

			@Override
			public ImageDTO extractData(ResultSet rs) throws SQLException, DataAccessException {
				if (rs.next()) {
					ImageDTO imageDTO = new ImageDTO();
					imageDTO.setHash(rs.getString(1));
					imageDTO.setExtension(rs.getString(2));
					imageDTO.setCid(rs.getString(3));
					imageDTO.setActive(rs.getBoolean(4));
					imageDTO.setAdded(rs.getTimestamp(5));
					return imageDTO;
				}
				return null;
			}
		}, cid);
	}
	
	public boolean validateExistingOperation(OpOperation op) {
		String js = formatter.opToJson(op);
		OpOperation existingOperation = getOperationByHash(op.getHash());
		if (existingOperation != null && !js.equals(formatter.opToJson(existingOperation))) {
			throw new IllegalArgumentException(String.format(
					"Operation is duplicated with '%s' hash but different content: \n'%s'\n'%s'", op.getHash(),
					formatter.opToJson(existingOperation).replace("\n", ""), js.replace("\n", "")));
		}
		return existingOperation != null;
	}
	
	public void insertOperation(OpOperation op) {
		PGobject pGobject = new PGobject();
		pGobject.setType("jsonb");
		String js = formatter.opToJson(op);
		try {
			pGobject.setValue(js);
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		byte[] bhash = SecUtils.getHashBytes(op.getHash());
		jdbcTemplate.update("INSERT INTO " + OPERATIONS_TABLE + "(hash, content) VALUES (?, ?)", 
				bhash, pGobject);
	}

	public OpOperation getOperationByHash(String hash) {
		final byte[] bhash = SecUtils.getHashBytes(hash);
		OpOperation[] res = new OpOperation[1];
		jdbcTemplate.query("SELECT content from " + OPERATIONS_TABLE + " where hash = ?" , new Object[] {bhash} , new RowCallbackHandler() {

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				res[0] = formatter.parseOperation(rs.getString(1));
			}
		});
		return res[0];
	}
	
	

}
