package org.openplacereviews.opendb.service;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.service.HistoryManager.HistoryObjectRequest;
import org.openplacereviews.opendb.service.HistoryManager.HistoryEdit;
import org.openplacereviews.opendb.dto.ResourceDTO;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.openplacereviews.opendb.service.DBSchemaManager.*;
import static org.openplacereviews.opendb.service.HistoryManager.HISTORY_BY_OBJECT;
import static org.openplacereviews.opendb.service.HistoryManager.HISTORY_BY_TYPE;
import static org.openplacereviews.opendb.service.HistoryManager.HISTORY_BY_USER;

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

	@Autowired
	private HistoryManager historyManager;

	private Map<String, OpBlock> blocks = new ConcurrentHashMap<String, OpBlock>();
	private Map<String, OpBlock> orphanedBlocks = new ConcurrentHashMap<String, OpBlock>();
	private Map<String, SuperblockDbAccess> dbSuperBlocks = new ConcurrentHashMap<>();
	private OpBlockChain dbManagedChain = null;

	public Map<String, OpBlock> getOrphanedBlocks() {
		return orphanedBlocks;
	}

	protected String getHexFromPgObject(PGobject o) {
		String s = o.getValue();
		if (s == null) {
			return "";
		}
		if (!s.startsWith("\\x")) {
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
		if (!topBlockInfo.isEmpty()) {
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
		if (topBlockInfo.size() == 0) {
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
				new Object[]{SecUtils.getHashBytes(blockHash)}, new RowMapper<OpBlock>() {

					@Override
					public OpBlock mapRow(ResultSet rs, int rowNum) throws SQLException {
						OpBlock rawBlock = formatter.parseBlock(rs.getString(1));
						rawBlock.makeImmutable();
						return rawBlock;
					}

				});
		if (blocks.size() > 1) {
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

			for (String objTable : dbSchema.getObjectTables()) {
				jdbcTemplate.update("UPDATE " + objTable + " set superblock = ?  WHERE superblock = ? ", sbHashNew, sbHashCurrent);
				jdbcTemplate.update("UPDATE " + objTable + " set superblock = ?  WHERE superblock = ? ", sbHashNew, sbHashParent);
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
		private final byte[] sbhash;
		private volatile boolean staleAccess;

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
				if (staleAccess) {
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
						dbSchema.generatePKString(table, "p%1$d = ?", " and ", sz) +
						" order by sblockid desc";
				return jdbcTemplate.query(s, o, new ResultSetExtractor<OpObject>() {

					@Override
					public OpObject extractData(ResultSet rs) throws SQLException, DataAccessException {
						if (!rs.next()) {
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
			if (limit <= 0 && request.limit >= 0) {
				return Collections.emptyMap();
			}
			readLock.lock();
			try {
				if (staleAccess) {
					throw new UnsupportedOperationException();
				}
				Object[] o = new Object[2];
				o[0] = sbhash;
				o[1] = type;

				String objTable = dbSchema.getTableByType(type);
				final int keySize = dbSchema.getKeySizeByType(type);
				String sql = "select content, type, ophash, " + dbSchema.generatePKString(objTable, "p%1$d", ", ")
						+ "  from " + objTable + " where superblock = ? and type = ? ";
				if (limit > 0) {
					sql = sql + " limit " + limit;
				}
				Map<CompoundKey, OpObject> res = new LinkedHashMap<CompoundKey, OpObject>();
				jdbcTemplate.query(sql, o, new RowCallbackHandler() {
					List<String> ls = new ArrayList<String>(5);

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						ls.clear();
						for (int i = 0; i < keySize; i++) {
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
		public OpOperation getOperation(String rawHash) {
			readLock.lock();
			try {
				if (staleAccess) {
					throw new UnsupportedOperationException();
				}
				Object[] o = new Object[2];
				o[0] = sbhash;
				o[1] = SecUtils.getHashBytes(rawHash);
				String sql = "select d.content from " + OPERATIONS_TABLE + " d "
						+ " where d.superblock = ? and d.hash = ? ";
				OpOperation[] op = new OpOperation[1];
				jdbcTemplate.query(sql, o, new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if(rs.next()) {
							op[0] = formatter.parseOperation(rs.getString(1));
						}
					}


				});
				return op[0];
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
		OpBlockChain[] res = new OpBlockChain[]{OpBlockChain.NULL};
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
				if (!OUtils.isEmpty(pblockHash) && parentBlockHeader == null) {
					LOGGER.error(String.format("Orphaned block '%s' without parent '%s'.", blockHash, pblockHash));
					orphanedBlocks.put(blockHash, blockHeader);
					return;
				} else if (OUtils.isEmpty(superblock)) {
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
		if (bi.getBlockId() == 0) {
			return lastBlockRawHash.length() == 0;
		}
		if (bi.getRawHash().equals(lastBlockRawHash)) {
			return true;
		}
		String prevRawHash = bi.getPrevRawHash();
		OpBlock parentBlock = blocks.get(prevRawHash);
		if (parentBlock != null) {
			return isConnected(parentBlock, lastBlockRawHash);
		}
		return false;
	}

	private LinkedList<OpBlock> selectTopBlockFromOrphanedBlocks(OpBlockChain prev) {
		OpBlock topBlockInfo = null;
		String lastBlockRawHash = prev.getLastBlockRawHash();
		for (OpBlock bi : orphanedBlocks.values()) {
			boolean isNewer = false;
			if (topBlockInfo == null) {
				isNewer = true;
			} else if (topBlockInfo.getBlockId() < bi.getBlockId()) {
				isNewer = true;
			} else if (topBlockInfo.getBlockId() == bi.getBlockId() && topBlockInfo.getRawHash().compareTo(bi.getRawHash()) > 0) {
				isNewer = true;
			}
			if (isNewer && isConnected(bi, lastBlockRawHash)) {
				topBlockInfo = bi;
			}
		}
		// returns in order from the oldest to the newest
		LinkedList<OpBlock> blockList = new LinkedList<OpBlock>();
		if (topBlockInfo != null && topBlockInfo.getBlockId() > prev.getLastBlockId()) {
			OpBlock blockInfo = topBlockInfo;
			LinkedList<String> blocksInfoLst = new LinkedList<String>();
			while (blockInfo != null) {
				if (OUtils.equals(blockInfo.getRawHash(), lastBlockRawHash)) {
					return blockList;
				}
				orphanedBlocks.remove(blockInfo.getRawHash());
				blockList.addFirst(blockInfo);
				blocksInfoLst.addFirst(blockInfo.getRawHash());
				blockInfo = blocks.get(blockInfo.getPrevRawHash());
			}
			if (OUtils.isEmpty(lastBlockRawHash)) {
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
		while (!blc.isNullBlock() && !blc.isDbAccessed()) {
			beforeLast = lastNotSaved;
			lastNotSaved = blc;
			if (parentInOrphanedList) {
				for (OpBlock header : blc.getSuperblockHeaders()) {
					OpBlock existing = orphanedBlocks.remove(header.getRawHash());
					if (existing == null) {
						parentInOrphanedList = false;
						break;
					}
				}
			}
			blc = blc.getParent();
		}
		if (lastNotSaved != null && lastNotSaved.getSuperblockSize() >= superblockSize) {
			OpBlockChain saved = saveSuperblock(lastNotSaved);
			if (beforeLast != null) {
				if (!beforeLast.changeToEqualParent(saved)) {
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
			for (OpBlock block : blc.getSuperblockFullBlocks()) {
				byte[] blHash = SecUtils.getHashBytes(block.getFullHash());
				// assign parent hash only for last block
				// String blockRawHash = SecUtils.hexify(blHash);
				// LOGGER.info(String.format("Update block %s to superblock %s ", o.getHash(), superBlockHash));
				jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = ? where hash = ?", superBlockHash, blHash);
				int order = 0;
				int bid = block.getBlockId();
				for (OpOperation op : block.getOperations()) {
					long l = OUtils.combine(bid, order);
					opsId.put(op.getRawHash(), l);
					jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = ?, sblockid = ?, sorder = ? where hash = ?",
							superBlockHash, bid, order, SecUtils.getHashBytes(op.getRawHash()));
					order++;
				}
			}

			Map<String, Map<CompoundKey, OpObject>> so = blc.getSuperblockObjects();
			for (String type : so.keySet()) {
				Map<CompoundKey, OpObject> objects = so.get(type);
				List<Object[]> insertBatch = prepareInsertObjBatch(objects, type, superBlockHash, opsId);
				String table = dbSchema.getTableByType(type);
				dbSchema.insertObjIntoTableBatch(insertBatch, table, jdbcTemplate);
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

	protected List<Object[]> prepareInsertObjBatch(Map<CompoundKey, OpObject> objects, String type,
												   byte[] superBlockHash, Map<String, Long> opsId) {

		List<Object[]> insertBatch = new ArrayList<>(objects.size());
		int ksize = dbSchema.getKeySizeByType(type);
		Iterator<Entry<CompoundKey, OpObject>> it = objects.entrySet().iterator();
		while (it.hasNext()) {
			Entry<CompoundKey, OpObject> e = it.next();
			CompoundKey pkey = e.getKey();
			OpObject obj = e.getValue();
			long l = opsId.get(obj.getParentHash());
			int sblockid = OUtils.first(l);
			int sorder = OUtils.second(l);

			if (pkey.size() > ksize) {
				throw new UnsupportedOperationException("Key is too long to be stored: " + pkey.toString());
			}

			Object[] args = new Object[6 + ksize];
			args[0] = type;
			String ophash = obj.getParentHash();
			args[1] = SecUtils.getHashBytes(ophash);
			args[2] = superBlockHash;

			args[3] = sblockid;
			args[4] = sorder;
			PGobject contentObj = new PGobject();
			contentObj.setType("jsonb");
			try {
				contentObj.setValue(formatter.objToJson(obj));
			} catch (SQLException es) {
				throw new IllegalArgumentException(es);
			}
			args[5] = contentObj;
			pkey.toArray(args, 6);

			insertBatch.add(args);
		}

		return insertBatch;
	}

	public synchronized OpBlockChain compact(int prevSize, OpBlockChain blc, boolean db) {
		if (blc == null || blc.isNullBlock() || blc.getParent().isNullBlock()) {
			return blc;
		}
		OpBlockChain compactedParent = null;
		if (blc.isDbAccessed() == blc.getParent().isDbAccessed()) {
			compactedParent = compact(blc.getSuperblockSize(), blc.getParent(), db);
			// only 1 compact at a time
			boolean compact = compactedParent == blc.getParent();
			compact = compact && ((double) blc.getSuperblockSize() + compactCoefficient * prevSize) > ((double) blc.getParent().getSuperblockSize());
			if (compact) {
				LOGGER.info("Chain to compact: ");
				printBlockChain(blc);
				// See @SimulateSuperblockCompactSequences
				if (blc.isDbAccessed() && db) {
					// here we need to lock all db access of 2 blocks and run update in 1 transaction
					return compactTwoDBAccessed(blc);
				} else {
					LOGGER.info(String.format("Compact runtime superblock '%s' into  superblock '%s' ", blc.getParent().getSuperBlockHash(), blc.getSuperBlockHash()));
					blc = new OpBlockChain(blc, blc.getParent(), blc.getRules());
					return blc;
				}

			}
		} else {
			// redirect compact to parent
			compactedParent = compact(0, blc.getParent(), db);
		}
		if (blc.getParent() != compactedParent) {
			blc.changeToEqualParent(compactedParent);
		}
		return blc;
	}

	private void printBlockChain(OpBlockChain blc) {
		List<String> superBlocksChain = new ArrayList<String>();
		OpBlockChain p = blc;
		while (p != null && !p.isNullBlock()) {
			String sh = p.getSuperBlockHash();
			if (sh.startsWith("00")) {
				while (sh.startsWith("00")) {
					sh = sh.substring(2);
				}
				sh = "0*" + sh;
			}
			if (sh.length() > 10) {
				sh = sh.substring(0, 10);
			}
			if (p.isDbAccessed()) {
				sh = "db-" + sh;
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
			if (upd != 0) {
				// to do:
				// here we need to decide what to do with operations with empty blocks[] (they will be added to the queue after restart otherwise)
				for (OpOperation o : block.getOperations()) {
					jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE
									+ " set blocks = array_remove(blocks, ?) where hash = ?",
							blockHash, SecUtils.getHashBytes(o.getRawHash()));
				}
				jdbcTemplate.update("DELETE FROM " + OP_OBJ_HISTORY_TABLE + " WHERE blockhash = ?", SecUtils.getHashBytes(block.getFullHash()));
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
		if (blc.isDbAccessed()) {
			SuperblockDbAccess dba = dbSuperBlocks.get(blc.getSuperBlockHash());
			OpBlockChain res = new OpBlockChain(blc.getParent(), blc.getRules());
			List<OpBlock> lst = new ArrayList<OpBlock>(blc.getSuperblockFullBlocks());
			byte[] blockHash = SecUtils.getHashBytes(blc.getSuperBlockHash());
			Collections.reverse(lst);
			for (OpBlock block : lst) {
				res.replicateBlock(block);
			}
			boolean txRollback = false;
			try {
				dba.markAsStale(true);
				jdbcTemplate.execute("BEGIN");
				txRollback = true;
				jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = NULL where superblock = ?", blockHash);
				jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = NULL where superblock = ? ", blockHash);
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

	public ResourceDTO storeResourceObject(ResourceDTO imageDTO) throws IOException {
		imageDTO.setHash(SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, imageDTO.getMultipartFile().getBytes()));

		if (getResourceObjectIfExists(imageDTO) == null) {
			imageDTO.setAdded(new Date());
			jdbcTemplate.update("INSERT INTO " + EXT_RESOURCE_TABLE + "(hash, extension, cid, active, added) VALUES (?, ?, ?, ?, ?)",
					SecUtils.getHashBytes(imageDTO.getHash()), imageDTO.getExtension(), imageDTO.getCid(), imageDTO.isActive(), imageDTO.getAdded());
		}

		return imageDTO;
	}

	public ResourceDTO getResourceObjectIfExists(ResourceDTO imageDTO) {
		return jdbcTemplate.query("SELECT hash, extension, cid, active, added FROM " + EXT_RESOURCE_TABLE + " WHERE hash = ?", new ResultSetExtractor<ResourceDTO>() {

			@Override
			public ResourceDTO extractData(ResultSet rs) throws SQLException, DataAccessException {
				if (rs.next()) {
					ResourceDTO imageDTO = new ResourceDTO();
					imageDTO.setHash(SecUtils.hexify(rs.getBytes(1)));
					imageDTO.setExtension(rs.getString(2));
					imageDTO.setCid(rs.getString(3));
					imageDTO.setActive(rs.getBoolean(4));
					imageDTO.setAdded(rs.getTimestamp(5));
					return imageDTO;
				}
				return null;
			}
		}, new Object[]{SecUtils.getHashBytes(imageDTO.getHash())});
	}

	public List<ResourceDTO> getResources(boolean status, int addedMoreThanSecondsAgo) {
		return jdbcTemplate.query("SELECT cid, hash, extension FROM " + EXT_RESOURCE_TABLE + " WHERE active = ? AND added < ?", new ResultSetExtractor<List<ResourceDTO>>() {
			@Override
			public List<ResourceDTO> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<ResourceDTO> resources = new LinkedList<>();
				while (rs.next()) {
					ResourceDTO imageDTO = new ResourceDTO();
					imageDTO.setCid(rs.getString(1));
					imageDTO.setHash(SecUtils.hexify(rs.getBytes(2)));
					imageDTO.setExtension(rs.getString(3));
					resources.add(imageDTO);
				}
				return resources;
			}
		}, status, DateUtils.addSeconds(new Date(), -addedMoreThanSecondsAgo));
	}

	public void removeResObjectFromDB(ResourceDTO resDTO) {
		jdbcTemplate.update("DELETE FROM " + EXT_RESOURCE_TABLE + " WHERE hash = ?", new Object[]{SecUtils.getHashBytes(resDTO.getHash())});
	}

	public void updateImageActiveStatus(ResourceDTO imageDTO, boolean status) {
		jdbcTemplate.update("UPDATE " + EXT_RESOURCE_TABLE + " SET active = ? WHERE hash = ?", status, SecUtils.getHashBytes(imageDTO.getHash()));
	}

	public Long getAmountResourcesInDB() {
		return jdbcTemplate.query("SELECT COUNT(*) FROM " + EXT_RESOURCE_TABLE, new ResultSetExtractor<Long>() {
			@Override
			public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
				if (rs.next()) {
					return rs.getLong(1);
				}

				return null;
			}
		});
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

	public void saveHistoryForOperationObjects(List<Object[]> allBatches) {
		dbSchema.insertObjIntoHistoryTableBatch(allBatches, OP_OBJ_HISTORY_TABLE, jdbcTemplate);
	}

	public void retrieveHistory(HistoryObjectRequest historyObjectRequest) {
		String sql;
		switch (historyObjectRequest.historyType) {
			case HISTORY_BY_USER: {
				sql = "SELECT u1, u2, p1, p2, p3, p4, p5, time, obj, type, status from " + OP_OBJ_HISTORY_TABLE + " where " +
						dbSchema.generatePKString(OP_OBJ_HISTORY_TABLE, "u%1$d = ?", " AND ", historyObjectRequest.key.size()) +
						" ORDER BY time,sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
				loadHistory(sql, historyObjectRequest);
				break;
			}
			case HISTORY_BY_OBJECT: {
				String objType = null;
				if (historyObjectRequest.key.size() > 1) {
					objType = historyObjectRequest.key.get(0);
				}
				sql = "SELECT u1, u2, p1, p2, p3, p4, p5, time, obj, type, status FROM " + OP_OBJ_HISTORY_TABLE + " WHERE " +
						(objType == null ? "" : " type = ? AND ") + dbSchema.generatePKString(OP_OBJ_HISTORY_TABLE, "p%1$d = ?", " AND ",
								(objType == null ? historyObjectRequest.key.size() : historyObjectRequest.key.size() - 1)) +
						" ORDER BY time, sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
				loadHistory(sql, historyObjectRequest);
				break;
			}
			case HISTORY_BY_TYPE: {
				sql = "SELECT u1, u2, p1, p2, p3, p4, p5, time, obj, type, status FROM " + OP_OBJ_HISTORY_TABLE +
						" WHERE type = ?" + " ORDER BY time, sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
				loadHistory(sql, historyObjectRequest);
				break;
			}
		}
	}

	protected void loadHistory(String sql, HistoryObjectRequest historyObjectRequest) {
		historyObjectRequest.historySearchResult = jdbcTemplate.query(sql, historyObjectRequest.key.toArray(), new ResultSetExtractor<Map<List<String>, List<HistoryEdit>>>() {
			@Override
			public Map<List<String>, List<HistoryEdit>> extractData(ResultSet rs) throws SQLException, DataAccessException {
				Map<List<String>, List<HistoryEdit>> result = new LinkedHashMap<>();

				List<List<String>> objIds = new ArrayList<>();
				List<HistoryEdit> allObjects = new LinkedList<>();
				while (rs.next()) {
					List<String> users = new ArrayList<>();
					for (int i = 1; i <= USER_KEY_SIZE; i++) {
						if (rs.getString(i) != null) {
							users.add(rs.getString(i));
						}
					}
					List<String> ids = new ArrayList<>();
					for (int i = 3; i <= USER_KEY_SIZE + MAX_KEY_SIZE; i++) {
						if (rs.getString(i) != null) {
							ids.add(rs.getString(i));
						}
					}
					HistoryEdit historyObject = new HistoryEdit(
							users,
							rs.getString(10),
							formatter.parseObject(rs.getString(9)),
							formatFullDate(rs.getTimestamp(8)),
							HistoryManager.Status.getStatus(rs.getInt(11))
					);
					if (historyObject.getStatus().equals(HistoryManager.Status.EDITED)) {
						historyObject.setObjEdit(formatter.fromJsonToTreeMap(rs.getString(9)));
					}
					historyObject.setId(ids);

					allObjects.add(historyObject);
					if (!objIds.contains(ids)) {
						objIds.add(ids);
					}
				}
				generateObjMapping(objIds, allObjects, result);

				result = historyManager.generateHistoryObj(result, historyObjectRequest.sort);
				return result;
			}
		});
	}

	private void generateObjMapping(List<List<String>> objIds, List<HistoryEdit> allObjects, Map<List<String>, List<HistoryEdit>> history) {
		for (List<String> id : objIds) {
			List<HistoryEdit> objWithSameId = new LinkedList<>();
			for (HistoryEdit hdto : allObjects) {
				if (hdto.getId().equals(id)) {
					objWithSameId.add(hdto);
				}
			}
			history.put(id, objWithSameId);
		}
	}

	private String formatFullDate(Date date) {
		if (date == null)
			return null;

		return new DateTime(date).toString(OpObject.DATE_FORMAT);
	}

	public OpOperation getOperationByHash(String hash) {
		final byte[] bhash = SecUtils.getHashBytes(hash);
		OpOperation[] res = new OpOperation[1];
		jdbcTemplate.query("SELECT content from " + OPERATIONS_TABLE + " where hash = ?", new Object[]{bhash}, new RowCallbackHandler() {

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				res[0] = formatter.parseOperation(rs.getString(1));
			}
		});
		return res[0];
	}


}
