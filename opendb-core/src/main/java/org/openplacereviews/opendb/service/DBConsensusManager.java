package org.openplacereviews.opendb.service;


import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.BlockDbAccessInterface;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.IPFSService.ResourceDTO;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.openplacereviews.opendb.service.DBSchemaManager.*;

@Service
public class DBConsensusManager {

	protected static final Log LOGGER = LogFactory.getLog(DBConsensusManager.class);

	// check SimulateSuperblockCompactSequences to verify numbers

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private TransactionTemplate txTemplate;
	
	@Autowired
	private DBSchemaManager dbSchema;

	@Autowired
	private FileBackupManager backupManager;

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private LogOperationService logSystem;

	@Autowired
	private SettingsManager settingsManager;

	private Map<String, OpBlock> blocks = new ConcurrentHashMap<String, OpBlock>();
	private Map<String, OpBlock> orphanedBlocks = new ConcurrentHashMap<String, OpBlock>();
	private Map<String, SuperblockDbAccess> dbSuperBlocks = new ConcurrentHashMap<>();
	private OpBlockChain dbManagedChain = null;
	private OpBlockchainRules rules;


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
	public OpBlockChain init(MetadataDb metadataDB) {
		settingsManager.initPreferences();
		dbSchema.initializeDatabaseSchema(metadataDB, jdbcTemplate);
		backupManager.init();
		rules = new OpBlockchainRules(formatter, logSystem);
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
	
	public OpIndexColumn getIndex(String type, String columnId) {
		return dbSchema.getIndex(type, columnId);
	}
	
	public Collection<OpIndexColumn> getIndicesForType(String type) {
		return dbSchema.getIndicesForType(type);
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
		List<OpBlock> blockHeaders = new ArrayList<OpBlock>();
		blockHeaders.addAll(blc.getSuperblockHeaders());
		blockHeaders.addAll(blc.getParent().getSuperblockHeaders());
		String newSuperblockHash = OpBlockchainRules.calculateSuperblockHash(blockHeaders.size(), blc.getLastBlockRawHash());
		byte[] sbHashCurrent = SecUtils.getHashBytes(blc.getSuperBlockHash());
		byte[] sbHashParent = SecUtils.getHashBytes(blc.getParent().getSuperBlockHash());
		byte[] sbHashNew = SecUtils.getHashBytes(newSuperblockHash);
		return txTemplate.execute(new TransactionCallback<OpBlockChain>() {

			@Override
			public OpBlockChain doInTransaction(TransactionStatus status) {
				dbSB.markAsStale(true);
				dbPSB.markAsStale(true);
				jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashCurrent);
				jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashParent);

				jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashCurrent);
				jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = ? WHERE superblock = ? ", sbHashNew, sbHashParent);

				for (String objTable : dbSchema.getObjectTables()) {
					jdbcTemplate.update("UPDATE " + objTable + " set superblock = ?  WHERE superblock = ? ", sbHashNew, sbHashCurrent);
					jdbcTemplate.update("UPDATE " + objTable + " set superblock = ?  WHERE superblock = ? ", sbHashNew, sbHashParent);
				}

				OpBlockChain res = new OpBlockChain(blc.getParent().getParent(),
						blockHeaders, createDbAccess(newSuperblockHash, blockHeaders), blc.getRules());
				// on catch 
//				dbSB.markAsStale(false);
//				dbPSB.markAsStale(false);
				return res;
			}
		});
	}



	protected class SuperblockDbSpliterator implements Spliterator<Map.Entry<CompoundKey, OpObject>> {

		private static final int BATCH_SIZE = 250;
		private SqlRowSet rs;
		private SuperblockDbAccess dbAccess;
		private final int keySize; 
		private LinkedList<Map.Entry<CompoundKey, OpObject>> results = new LinkedList<>();
		private boolean end;
		private boolean onlyKeys;
		SuperblockDbSpliterator(SuperblockDbAccess dbAccess, int keySize, boolean onlyKeys, SqlRowSet rowSet) throws DBStaleException {
			this.dbAccess = dbAccess;
			this.keySize = keySize;
			this.onlyKeys = onlyKeys;
			this.rs = rowSet;
			readEntries();
		}
		
		private boolean readEntries() throws DBStaleException {
			if (end) {
				return true;
			}
			dbAccess.readLock.lock();
			try {
				dbAccess.checkNotStale();
				final List<String> ls = new ArrayList<String>(5);
				int cnt = 0;
				while (cnt++ < BATCH_SIZE) {
					if(!rs.next()) {
						end = true;
						return true;
					}
					ls.clear();
					for (int i = 0; i < keySize; i++) {
						ls.add(rs.getString(i + 4));
					}
					final CompoundKey k = new CompoundKey(0, ls);
					final OpObject obj ;
					if(!onlyKeys) {
						String cont = rs.getString(1);
						obj = cont == null ? new OpObject(true) : formatter.parseObject(cont);
					} else {
						obj = new OpObject(rs.getBoolean(1));
					}
					obj.setParentOp(rs.getString(2), SecUtils.hexify((byte[]) rs.getObject(3)));
					results.add(new Map.Entry<CompoundKey, OpObject>() {

						@Override
						public CompoundKey getKey() {
							return k;
						}

						@Override
						public OpObject getValue() {
							return obj;
						}

						@Override
						public OpObject setValue(OpObject value) {
							throw new UnsupportedOperationException();
						}
					});
				}
			} finally {
				dbAccess.readLock.unlock();
			}
			return false;
		}

		@Override
		public boolean tryAdvance(Consumer<? super Map.Entry<CompoundKey, OpObject>> action) {
			boolean empty = results.isEmpty();
			if(empty) {
				readEntries();
				empty = results.isEmpty();
			}
			if(!empty) {
				action.accept(results.pop());
				return true;
			}
			return false;
		}

		@Override
		public Spliterator<Entry<CompoundKey, OpObject>> trySplit() {
			return null;
		}

		@Override
		public long estimateSize() {
			return Long.MAX_VALUE;
		}

		@Override
		public int characteristics() {
			return 0;
		}
		
	}
	
	
	public class DBStaleException extends RuntimeException {
		public DBStaleException(String string) {
			super(string);
		}

		private static final long serialVersionUID = 327630066462749799L;
		
	}
	
	protected class SuperblockDbAccess implements BlockDbAccessInterface {

		protected final String superBlockHash;
		protected final List<OpBlock> blockHeaders;
		private final ReentrantReadWriteLock readWriteLock;
		private final ReadLock readLock;
		private final byte[] sbhash;
		private AtomicBoolean staleAccess = new AtomicBoolean(false);

		public SuperblockDbAccess(String superBlockHash, Collection<OpBlock> blockHeaders) {
			this.superBlockHash = superBlockHash;
			sbhash = SecUtils.getHashBytes(superBlockHash);
			this.blockHeaders = new ArrayList<OpBlock>(blockHeaders);
			this.readWriteLock = new ReentrantReadWriteLock();
			readLock = this.readWriteLock.readLock();
			dbSuperBlocks.put(superBlockHash, this);
		}

		public boolean markAsStale(boolean stale) throws DBStaleException {
			WriteLock lock = readWriteLock.writeLock();
			lock.lock();
			try {
				staleAccess.set(stale);
				return true;
			} finally {
				lock.unlock();
			}
		}

		@Override
		public OpObject getObjectById(String type, CompoundKey k) throws DBStaleException {
			readLock.lock();
			try {
				checkNotStale();
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
						String cnt = rs.getString(1);
						OpObject obj;
						if(cnt == null) {
							obj = new OpObject(true);
						} else {
							obj = formatter.parseObject(cnt);
						}
						obj.setParentOp(rs.getString(2), SecUtils.hexify(rs.getBytes(3)));
						return obj;
					}
				});
			} finally {
				readLock.unlock();
			}
		}

		private void checkNotStale() throws DBStaleException {
			if (staleAccess.get()) {
				throw new DBStaleException("Superblock is stale : " + SecUtils.hexify(sbhash));
			}
		}

		public Stream<Map.Entry<CompoundKey, OpObject>> streamObjects(String type,  int limit, boolean onlyKeys, Object... extraParams) throws DBStaleException {
			readLock.lock();
			try {
				checkNotStale();
				int l = (extraParams == null ? 0 : extraParams.length);
				Object[] o = new Object[2 + Math.max(l - 1, 0)];
				o[0] = sbhash;
				o[1] = type;
				String cond = null;
				for(int i = 0; i < l; i++) {
					if(i == 0) {
						cond =  extraParams[i].toString();
					} else {
						o[1 + i] = extraParams[i];
					}
				}
				String objTable = dbSchema.getTableByType(type);
				final int keySize = dbSchema.getKeySizeByType(type);
				String cntField = "content";
				if(onlyKeys) {
					 cntField = "case when content is null then true else false end";
				}
				String sql = "select "+cntField+", type, ophash, " + dbSchema.generatePKString(objTable, "p%1$d", ", ")
						+ "  from " + objTable + " where superblock = ? and type = ? " + (cond == null ? "" : " and " + cond);
				// so the result can simply override and get latest version
				sql = sql + " order by sblockid asc";
				if (limit > 0) {
					sql = sql + " limit " + limit;
				}
				
				final SqlRowSet rs = jdbcTemplate.queryForRowSet(sql, o);
				return StreamSupport.stream(new SuperblockDbSpliterator(this, keySize, onlyKeys, rs), false); 
			} finally {
				readLock.unlock();
			}
			
		}
		
		public int countObjects(String type, Object... extraParams) throws DBStaleException {
			readLock.lock();
			try {
				checkNotStale();
				int l = (extraParams == null ? 0 : extraParams.length);
				Object[] o = new Object[2 + Math.max(l - 1, 0)];
				o[0] = sbhash;
				o[1] = type;
				String cond = null;
				for(int i = 0; i < l; i++) {
					if(i == 0) {
						cond =  extraParams[i].toString();
					} else {
						o[1 + i] = extraParams[i];
					}
				}
				String objTable = dbSchema.getTableByType(type);
				String sql = "select count(*) from " + objTable + 
						" where superblock = ? and type = ? " + (cond == null ? "" : " and " + cond); 
				return jdbcTemplate.queryForObject(sql, o, Number.class).intValue();
			} finally {
				readLock.unlock();
			}
			
		}

		@Override
		public OpOperation getOperation(String rawHash) throws DBStaleException {
			readLock.lock();
			try {
				checkNotStale();
				OpOperation[] op = new OpOperation[1];
				final byte[] ophash = SecUtils.getHashBytes(rawHash);
				jdbcTemplate.query("SELECT content from " + OPERATIONS_TABLE + " where superblock = ? and hash = ?", new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (rs.next()) {
							op[0] = formatter.parseOperation(rs.getString(1));
						}
					}
				}, sbhash, ophash);
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
				isSuperblockReferenceActive = !staleAccess.get();
			} finally {
				readLock.unlock();
			}
			if (isSuperblockReferenceActive) {
				// to do faster to load by superblock reference, so it could be 1 sql
				// but it need to handle correctly DBStaleException
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

		@Override
		public Collection<String> getObjectTypes() {
			readLock.lock();
			try {
				Set<String> types = new LinkedHashSet<String>();
				checkNotStale();
				jdbcTemplate.query("SELECT distinct type from " + OPERATIONS_TABLE + " where superblock = ?", new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (rs.next()) {
							types.add(rs.getString(1));
						}
					}
				}, sbhash);
				return types;
			} finally {
				readLock.unlock();
			}
		}

	}

	protected BlockDbAccessInterface createDbAccess(String superblock, Collection<OpBlock> blockHeaders) {
		return new SuperblockDbAccess(superblock, blockHeaders);
	}

	private OpBlockChain loadBlockHeadersAndBuildMainChain(final OpBlockchainRules rules) {
		OpBlockChain[] res = new OpBlockChain[]{OpBlockChain.NULL};
		jdbcTemplate.query("SELECT hash, phash, blockid, superblock, header, pg_column_size(content), opcount, " +
				"objdeleted, objedited, objadded from " + BLOCKS_TABLE + " order by blockId asc", new RowCallbackHandler() {

			LinkedList<OpBlock> blockHeaders = new LinkedList<OpBlock>();

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				String blockHash = SecUtils.hexify(rs.getBytes(1));
				String pblockHash = SecUtils.hexify(rs.getBytes(2));
				String superblock = SecUtils.hexify(rs.getBytes(4));
				OpBlock parentBlockHeader = blocks.get(pblockHash);
				OpBlock blockHeader = formatter.parseBlock(rs.getString(5));
				blockHeader.makeImmutable();
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
		OpBlock blockheader = OpBlock.createHeader(opBlock, rules);
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
		txTemplate.execute(new TransactionCallback<Void>() {

			@Override
			public Void doInTransaction(TransactionStatus status) {
				int added = 0, edited = 0, deleted = 0;
				for (OpOperation opOperation : opBlock.getOperations()) {
					added += opOperation.getCreated().size();
					edited += opOperation.getEdited().size();
					deleted += opOperation.getDeleted().size();
				}
				jdbcTemplate.update("INSERT INTO " + BLOCKS_TABLE
				+ " (hash, phash, blockid, header, content, opcount, objdeleted, objedited, objadded) " +
								"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", blockHash, prevBlockHash,
						opBlock.getBlockId(), blockHeaderObj, blockObj, opBlock.getOperations().size(), deleted, added, edited);
				for (OpOperation o : opBlock.getOperations()) {
					int upd = jdbcTemplate.update(
							"UPDATE " + OPERATIONS_TABLE + " set blocks = blocks || ? where hash = ?", 
							blockHash, SecUtils.getHashBytes(o.getHash()));
					if (upd == 0) {
						throw new IllegalArgumentException(
								String.format("Can't create block '%s' cause op '%s' doesn't exist",
										opBlock.getRawHash(), o.getHash()));
					}
				}
				return null;
			}
		});
		backupManager.insertBlock(opBlock);
		blocks.put(rawHash, blockheader);
		orphanedBlocks.put(rawHash, blockheader);
	}

	public OpBlockChain saveMainBlockchain(OpBlockChain blc) {
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
		if (lastNotSaved != null && lastNotSaved.getSuperblockSize() >= settingsManager.OPENDB_SUPERBLOCK_SIZE.get()) {
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
		
		
		return txTemplate.execute(new TransactionCallback<OpBlockChain>() {

			@Override
			public OpBlockChain doInTransaction(TransactionStatus status) {
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

				for (String type : blc.getRawSuperblockTypes()) {
					Stream<Map.Entry<CompoundKey, OpObject>> objects = blc.getRawSuperblockObjects(type);
					Collection<OpIndexColumn> indexes = dbSchema.getIndicesForType(type);
					List<OpIndexColumn> dbIndexes = new ArrayList<OpIndexColumn>();
					for (OpIndexColumn index : indexes) {
						if(index.getIdIndex() < 0) {
							dbIndexes.add(index);
						}
					}
					List<Object[]> insertBatch = prepareInsertObjBatch(objects, type, superBlockHash, opsId, dbIndexes);
					String table = dbSchema.getTableByType(type);
					dbSchema.insertObjIntoTableBatch(insertBatch, table, jdbcTemplate, dbIndexes);
				}
				OpBlockChain dbchain = new OpBlockChain(blc.getParent(), blockHeaders, createDbAccess(superBlockHashStr, blockHeaders),
						blc.getRules());

				return dbchain;
			}
		});
	}

	protected List<Object[]> prepareInsertObjBatch(Stream<Map.Entry<CompoundKey, OpObject>> objects, String type,
												   byte[] superBlockHash, Map<String, Long> opsId, Collection<OpIndexColumn> indexes) {

		List<Object[]> insertBatch = new ArrayList<>();
		int ksize = dbSchema.getKeySizeByType(type);
		Iterator<Entry<CompoundKey, OpObject>> it = objects.iterator();
		try {
			Connection conn = jdbcTemplate.getDataSource().getConnection();
			while (it.hasNext()) {
				Entry<CompoundKey, OpObject> e = it.next();
				CompoundKey pkey = e.getKey();
				OpObject obj = e.getValue();
				// OpObject.NULL doesn't have parent hash otherwise it should be a separate object
				Long l = opsId.get(obj.getParentHash());
				if (obj == OpObject.NULL) {
					l = 0l;
				}
				if(l == null) {
					throw new IllegalArgumentException(String.format("Not found op: '%s'", obj.getParentHash()));
				}
				int sblockid = OUtils.first(l);
				int sorder = OUtils.second(l);
				if (pkey.size() > ksize) {
					throw new UnsupportedOperationException("Key is too long to be stored: " + pkey.toString());
				}

				Object[] args = new Object[6 + ksize + indexes.size()];
				int ind = 0;
				args[ind++] = type;
				String ophash = obj.getParentHash();
				args[ind++] = SecUtils.getHashBytes(ophash);
				args[ind++] = superBlockHash;

				args[ind++] = sblockid;
				args[ind++] = sorder;
				if (!obj.isDeleted()) {
					PGobject contentObj = new PGobject();
					contentObj.setType("jsonb");
					try {
						contentObj.setValue(formatter.objToJson(obj));
					} catch (SQLException es) {
						throw new IllegalArgumentException(es);
					}
					args[ind++] = contentObj;
				} else {
					args[ind++] = null;
				}

				for (OpIndexColumn index : indexes) {
					if (!obj.isDeleted()) {
						args[ind++] = index.evalDBValue(obj, conn);
					} else {
						args[ind++] = null;
					}
				}
				pkey.toArray(args, ind);

				insertBatch.add(args);
			}
			conn.close();
		} catch (SQLException e) {
			throw new IllegalArgumentException();
		}

		return insertBatch;
	}

	public OpBlockChain compact(int prevSize, OpBlockChain blc, boolean db) {
		if (blc == null || blc.isNullBlock() || blc.getParent().isNullBlock()) {
			return blc;
		}
		OpBlockChain compactedParent = null;
		if (blc.isDbAccessed() == blc.getParent().isDbAccessed()) {
			compactedParent = compact(blc.getSuperblockSize(), blc.getParent(), db);
			// only 1 compact at a time
			boolean compact = compactedParent == blc.getParent();
			compact = compact && ((double) blc.getSuperblockSize() + settingsManager.OPENDB_COMPACT_COEFICIENT.get() * prevSize) > ((double) blc.getParent().getSuperblockSize());
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
		return txTemplate.execute(new TransactionCallback<Integer>() {

			@Override
			public Integer doInTransaction(TransactionStatus status) {
				int deleted = 0;
				// simple approach without using transaction isolations
				for (String op : ops) {
					deleted += jdbcTemplate.update(
							"WITH moved_rows AS ( DELETE FROM " + OPERATIONS_TABLE
									+ "     a WHERE hash = ? and (blocks = '{}' or blocks is null) RETURNING a.*) "
									+ " INSERT INTO " + OPERATIONS_TRASH_TABLE
									+ " (id, hash, time, type, content) SELECT dbid, hash, now(), type, content FROM moved_rows",
							SecUtils.getHashBytes(op));
				}
				return deleted;
			}
		});
	}

	public boolean removeFullBlock(OpBlock block) {
		return txTemplate.execute(new TransactionCallback<Boolean>() {

			@Override
			public Boolean doInTransaction(TransactionStatus status) {
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
				return upd != 0;
			}
			
		});
	}

	public OpBlockChain unloadSuperblockFromDB(OpBlockChain blc) {
		if (blc.isDbAccessed()) {
			SuperblockDbAccess dba = dbSuperBlocks.get(blc.getSuperBlockHash());
			final OpBlockChain res = new OpBlockChain(blc.getParent(), blc.getRules());
			List<OpBlock> lst = new ArrayList<OpBlock>(blc.getSuperblockFullBlocks());
			byte[] blockHash = SecUtils.getHashBytes(blc.getSuperBlockHash());
			Collections.reverse(lst);
			for (OpBlock block : lst) {
				res.replicateBlock(block);
			}
			return txTemplate.execute(new TransactionCallback<OpBlockChain>() {

				@Override
				public OpBlockChain doInTransaction(TransactionStatus status) {
					dba.markAsStale(true);
					jdbcTemplate.update("UPDATE " + OPERATIONS_TABLE + " set superblock = NULL where superblock = ?", blockHash);
					jdbcTemplate.update("UPDATE " + BLOCKS_TABLE + " set superblock = NULL where superblock = ? ", blockHash);
					for (String objTable : dbSchema.getObjectTables()) {
						jdbcTemplate.update("DELETE FROM " + objTable + " where superblock = ?", blockHash);
					}
					return res;
				}
			}) ;
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

	public void removeResource(ResourceDTO resDTO) {
		jdbcTemplate.update("DELETE FROM " + EXT_RESOURCE_TABLE + " WHERE hash = ?", new Object[]{SecUtils.getHashBytes(resDTO.getHash())});
	}

	public void updateResourceActiveStatus(ResourceDTO imageDTO, boolean status) {
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
		txTemplate.execute(new TransactionCallback<OpOperation>() {

			@Override
			public OpOperation doInTransaction(TransactionStatus status) {
				PGobject pGobject = new PGobject();
				pGobject.setType("jsonb");

				String js = formatter.opToJson(op);
				String type = op.getType();
				try {
					pGobject.setValue(js);
				} catch (SQLException e) {
					throw new IllegalArgumentException(e);
				}
				byte[] bhash = SecUtils.getHashBytes(op.getHash());

				jdbcTemplate.update("INSERT INTO " + OPERATIONS_TABLE + "(hash, type, content) VALUES (?, ?, ?)", bhash, type, pGobject);
				return op;
			}
			
		});
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

	public String getSetting(String key) {
		return dbSchema.getSetting(jdbcTemplate, key);
	}

	public TreeMap<String, Map<String, OpIndexColumn>> getIndices() {
		return dbSchema.getIndexes();
	}


}
