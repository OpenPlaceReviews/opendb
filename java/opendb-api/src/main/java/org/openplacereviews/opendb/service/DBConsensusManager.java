package org.openplacereviews.opendb.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
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
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.ValidationTimer;
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
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private JsonFormatter formatter;
	
	@Autowired
	private LogOperationService logSystem;

	private Map<String, SuperblockChain> superBlockChains = new ConcurrentHashMap<String, DBConsensusManager.SuperblockChain>();
	private Map<String, Integer> orphanedBlocks = new ConcurrentHashMap<String, Integer>();
	private SuperblockChain mainChain;
	
	public static class SuperblockChain {
		public final SuperblockChain parent;
		public final String superblockHash;
		
		private List<SuperblockChain> leafSuperBlocks = Collections.emptyList();
		final Set<String> blocksSet = new ConcurrentHashSet<String>();
		final Deque<String> blocks = new ConcurrentLinkedDeque<String>();
		
		public SuperblockChain(String superBlockHash, SuperblockChain parent) {
			this.parent = parent;
			this.superblockHash = superBlockHash;
		}
		
		public int getDepth() {
			int d = blocks.size();
			if(parent != null) {
				d += parent.getDepth();
			}
			return d;
		}
		
		public String calculateRawSuperBlockHash() {
			if(blocks.size() == 0) {
				return "";
			}
			ByteArrayOutputStream bts = new ByteArrayOutputStream(blocks.size() * 256);
			Iterator<String> ds = blocks.descendingIterator();
			while(ds.hasNext()) {
				byte[] hashBytes = SecUtils.getHashBytes(ds.next());
				try {
					bts.write(hashBytes);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}
			return SecUtils.hexify(SecUtils.calculateHash(SecUtils.HASH_SHA256, bts.toByteArray(), null));
		}

		public boolean containsBlock(String block) {
			if(blocksSet.contains(block)) {
				return true;
			}
			if(parent != null) {
				return parent.containsBlock(block);
			}
			return false;
		}

		public List<SuperblockChain> getAllFromBottom(List<SuperblockChain> l) {
			if (parent == null) {
				if (l == null) {
					l = new ArrayList<SuperblockChain>();
				}
			} else {
				l = parent.getAllFromBottom(l);
			}
			l.add(this);
			return l;
		}
	}
	
	
	//////////// SYSTEM TABLES DDL ////////////
	protected static final String DDL_CREATE_TABLE_BLOCKS = "create table blocks (hash bytea PRIMARY KEY, phash bytea, blockid int, superblocks bytea[], psuperblocks bytea[], details jsonb)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_HASH = "create index blocks_hash_ind on blocks(hash)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_PHASH = "create index blocks_phash_ind on blocks(phash)";
	protected static final String DDL_CREATE_TABLE_BLOCK_INDEX_BLOCKID = "create index blocks_blockid_ind on blocks(blockid)";
	
	
	protected static final String DDL_CREATE_TABLE_OPS = "create table operations (dbid serial not null, hash bytea PRIMARY KEY, blocks bytea[], details jsonb)";
	protected static final String DDL_CREATE_TABLE_OPS_INDEX_HASH = "create index operations_hash_ind on operations(hash)";
	// Query / insert values 
	// select encode(b::bytea, 'hex') from test where b like (E'\\x39')::bytea||'%';
	// insert into test(b) values (decode('39556d070fd95f54b554010207d42605a8d0adfbb3b8b8e134df7df0689d78ab', 'hex');

	public static void main(String[] args) {
		System.out.println(DDL_CREATE_TABLE_BLOCKS + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_HASH + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_PHASH + ';');
		System.out.println(DDL_CREATE_TABLE_BLOCK_INDEX_BLOCKID + ';');
		
		System.out.println(DDL_CREATE_TABLE_OPS + ';');
		System.out.println(DDL_CREATE_TABLE_OPS_INDEX_HASH + ';');
	}
	
	
	
	public Map<String, Integer> getOrphanedBlocks() {
		return orphanedBlocks;
	}
	
	public List<SuperblockChain> getLeafSuperblocks() {
		List<SuperblockChain> list = new ArrayList<DBConsensusManager.SuperblockChain>();
		for(SuperblockChain lc :  superBlockChains.values()) {
			if(lc.leafSuperBlocks.isEmpty()) {
				list.add(lc);
			}
		}
		return list;
	}
	
	private SuperblockChain getOrCreateSuperblock(String superBlockHash, String psuperBlockHash, boolean trace) {
		SuperblockChain sc = superBlockChains.get(superBlockHash);
		if(sc == null) {
			SuperblockChain parent = superBlockChains.get(psuperBlockHash);
			sc = new SuperblockChain(superBlockHash, parent);
			if(parent != null) {
				List<SuperblockChain> lst = new ArrayList<>(parent.leafSuperBlocks);
				lst.add(sc);
			}
			if(trace) {
				LOGGER.debug(String.format("Create superblock %s parent %s ", superBlockHash, psuperBlockHash));
			}
			superBlockChains.put(superBlockHash, sc);
		}
		return sc;
	}
	
	public OpBlockChain init(MetadataDb metadataDB) {
		LOGGER.info("... Loading superblocks ...");
		ValidationTimer timer = new ValidationTimer();
		timer.start();
		
		int[] blocksNumber = new int[1];
		jdbcTemplate.query("SELECT hash, blockid, superblocks, psuperblocks from blocks order by blockId asc", new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				blocksNumber[0]++;
				Object[] superBlocks = (Object[]) rs.getArray(3).getArray();
				Object[] psuperBlocks = (Object[]) rs.getArray(4).getArray();
				String blockHash = SecUtils.hexify(rs.getBytes(1));
				int blockId = rs.getInt(2);
				if(superBlocks == null || superBlocks.length == 0) {
					orphanedBlocks.put(blockHash, blockId);
				} else {
					for(int i = 0; i < superBlocks.length; i++) {
						String superBlockHash = getHexFromPgObject((PGobject) superBlocks[i]);
						String psuperBlockHash = getHexFromPgObject((PGobject) psuperBlocks[i]);
						SuperblockChain sc = getOrCreateSuperblock(superBlockHash, psuperBlockHash, false);
						sc.blocksSet.add(blockHash);
						sc.blocks.add(blockHash);
					}
				}
			}

			private String getHexFromPgObject(PGobject o) {
				String s = o.getValue();
				if(s == null) {
					return "";
				}
				if(!s.startsWith("\\x")) {
					throw new UnsupportedOperationException();
				}
				return s.substring(2);
			}

			
		});
		for(SuperblockChain b : superBlockChains.values()) {
			if(!b.superblockHash.endsWith(b.calculateRawSuperBlockHash())) {
				throw new IllegalStateException(String.format("Super block hash '%s' doesn't match calculated superblock hash with blocks it conains '%s'.",
						b.superblockHash, b.calculateRawSuperBlockHash()));
			}
		}
		LOGGER.info(String.format("... Loaded %d superblocks with %d blocks ...", superBlockChains.size(),  
				blocksNumber[0]));
		
		List<SuperblockChain> leafSuperBlocks = getLeafSuperblocks();
		int mainDepth = 0;
		for(SuperblockChain s : leafSuperBlocks) {
			int dp = s.getDepth();
			if(dp > mainDepth) {
				mainDepth = dp;
				mainChain = s;
			}
		}
		LOGGER.info(String.format("... Select main blockchain with %d depth out of %d chains ...", mainDepth, leafSuperBlocks.size()));
		

		OpBlockchainRules rules = new OpBlockchainRules(formatter, logSystem);
		OpBlockChain parent = OpBlockChain.NULL;
		
		if (mainChain != null) {
			List<SuperblockChain> allChains = mainChain.getAllFromBottom(null);
			int blocks = 0;
			for (SuperblockChain sc : allChains) {
				parent = new OpBlockChain(parent, rules);
				final OpBlockChain newParent = parent;
				blocks += sc.blocks.size();

				Iterator<String> ds = sc.blocks.iterator();
				while(ds.hasNext()) {
					String blockHash = ds.next();
					jdbcTemplate.query("SELECT details from blocks where hash = ? ",
							new Object[] { SecUtils.getHashBytes(blockHash) }, new RowCallbackHandler() {

								@Override
								public void processRow(ResultSet rs) throws SQLException {
									OpBlock rawBlock = formatter.parseBlock(rs.getString(1));
									OpBlock replicateBlock = newParent.replicateBlock(rawBlock);
									if (replicateBlock == null) {
										throw new IllegalStateException("Could not replicate block: "
												+ formatter.toJson(rawBlock));
									}
								}
							});
				}
			}
			LOGGER.info(String.format("... Loaded %d blocks ...", blocks));
		}
		OpBlockChain blcQueue = new OpBlockChain(parent, rules);
		LOGGER.info("... Loading operation queue  ...");
		int[] ops = new int[1];
		jdbcTemplate.query("SELECT details from operations where blocks is null order by dbid asc ", new RowCallbackHandler(){

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				ops[0]++;
				blcQueue.addOperation(formatter.parseOperation(rs.getString(1)));
			}
			
		});
		LOGGER.info(String.format("... Loaded operation %d into queue  ...", ops[0]));
		LOGGER.info(String.format("+++ Database blockchain initialized +++"));
		return blcQueue;
	}
	
	public void insertBlock(OpBlock opBlock) {
		PGobject pGobject = new PGobject();
		pGobject.setType("jsonb");
		try {
			pGobject.setValue(formatter.toJson(opBlock));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		byte[] blockHash = SecUtils.getHashBytes(opBlock.getHash());
		byte[] prevBlockHash = SecUtils.getHashBytes(opBlock.getStringValue(OpBlock.F_PREV_BLOCK_HASH));
		jdbcTemplate.update("INSERT INTO blocks(hash, phash, blockid, details) VALUES (?, ?, ?, ?)" , 
				blockHash, prevBlockHash, opBlock.getBlockId(), pGobject);
		for(OpOperation o : opBlock.getOperations()) {
			jdbcTemplate.update("UPDATE operations set blocks = blocks || ? where hash = ?" , 
					blockHash, SecUtils.getHashBytes(o.getHash()));	
		}
	}
	
	public void saveMainBlockchain(OpBlockChain blc) {
		mainChain = saveBlockchain(blc);
	}
	
	private SuperblockChain saveBlockchain(OpBlockChain blc) {
		if(blc != null && OUtils.isEmpty(blc.getSuperBlockHash())) {
			blc = blc.getParent();
		}
		if(blc != null) {
			SuperblockChain parent = saveBlockchain(blc.getParent());
			String psuperBlockHash = parent == null ? "" : parent.superblockHash;
			// TODO make unique superBlockHash 
			String superBlockHash = blc.getSuperBlockHash();
			SuperblockChain sc = getOrCreateSuperblock(superBlockHash, psuperBlockHash, true);
			// size is not equal means it was created
			if(sc.blocks.size() != blc.getSuperblockSize()) {
				byte[] shash = SecUtils.getHashBytes(superBlockHash);
				byte[] phash = SecUtils.getHashBytes(psuperBlockHash);
				byte[] empty = new byte[0];
				Iterator<OpBlock> it = blc.getOneSuperBlock().iterator();
				while(it.hasNext()) {
					OpBlock o = it.next();
					// assign parent hash only for last block
					// LOGGER.info(String.format("Update block %s to superblock %s ", o.getHash(), superBlockHash));
					jdbcTemplate.update("UPDATE blocks set superblocks = superblocks || ?, psuperblocks = psuperblocks || ? where hash = ?" , 
							shash, it.hasNext() ? empty : phash, SecUtils.getHashBytes(o.getHash()));
					sc.blocks.addFirst(o.getHash());
					sc.blocksSet.add(o.getHash());
				}
				
			}
			return sc;
		}
		return null;
	}
	
	public void insertOperation(OpOperation op) {
		PGobject pGobject = new PGobject();
		pGobject.setType("jsonb");
		try {
			pGobject.setValue(formatter.toJson(op));
		} catch (SQLException e) {
			throw new IllegalArgumentException(e);
		}
		jdbcTemplate.update("INSERT INTO operations(hash, details) VALUES (?, ?)" , 
				SecUtils.getHashBytes(op.getHash()), pGobject);
	}
	

	protected boolean createTable(OpOperation definition) {
		String tableName = definition.getStringValue(FIELD_NAME);
		Map<String, String> tableColumns = new TreeMap<String, String>();
		StringBuilder sql = new StringBuilder("create table " + tableName);
		StringBuilder columnsDef = new StringBuilder();
		for(Entry<String, String> e : tableColumns.entrySet()) {
			if(columnsDef.length() > 0) {
				columnsDef.append(", ");
			}
			columnsDef.append(e.getKey()).append(" ").append(e.getValue());
		}
		sql.append("(").append(columnsDef).append(")");
		try {
			LOGGER.info("DDL executed: " + sql);
			jdbcTemplate.execute(sql.toString());
		} catch(RuntimeException e) {
			LOGGER.warn("DDL failed: " + e.getMessage(), e);
			throw e;
		}
		return true;
	}
	
		
	

	



}
