package org.openplacereviews.opendb.service.bots;


import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.ops.de.CompoundKey;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.OUtils;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class GenericBlockchainReviewBot<T> extends GenericMultiThreadBot<T> {

	private static final String F_BLOCK_HASH = "lastblock";
	
	@Autowired
	private BlocksManager blocksManager;

	private int totalCnt = 1;
	private int progress = 0;

	private int changed;
	
	public GenericBlockchainReviewBot(OpObject botObject) {
		super(botObject);
	}
	
	protected String botTypeName() {
		return getTaskName();
	}
	
	public abstract String objectType();
	
	public abstract boolean processSingleObject(OpObject value, OpOperation op, OpBlock lastBlockHeader);
	
	@SuppressWarnings("unchecked")
	@Override
	public synchronized T call() throws Exception {
		addNewBotStat();
		super.initVars();
		try {
			OpBlockChain blc = blocksManager.getBlockchain();
			OpBlockChain init = blc;
			totalCnt = blc.countAllObjects(objectType());
			progress = 0;
			changed = 0;
			String lastScannedBlockHash = botObject.getField(null, F_BOT_STATE, F_BLOCK_HASH);
			info(String.format("Synchronization of '%s' has started from block %s. Total %d places.", botTypeName(), lastScannedBlockHash, totalCnt));
			OpOperation op = initOpOperation(objectType());
			Set<CompoundKey> keys = new HashSet<CompoundKey>();
			boolean blockExist = blc.getBlockHeaderByRawHash(wrapNull(lastScannedBlockHash)) != null;
			while (blc != null && !blc.isNullBlock()) {
				OpBlock lastBlockHeader = blc.getLastBlockHeader();
				Iterator<Entry<CompoundKey, OpObject>> it = blc.getRawSuperblockObjects("opr.place").iterator();
				while (it.hasNext()) {
					Entry<CompoundKey, OpObject> e = it.next();
					if (!keys.add(e.getKey())) {
						continue;
					}
					progress++;
					boolean proc = processSingleObject(e.getValue(), op, lastBlockHeader);
					if (proc) {
						op = addOpIfNeeded(op, false);
						changed++;
					}
					if (progress % 5000 == 0) {
						info(String.format("Progress of '%s' %d / %d  (changed %d).", botTypeName(),
								progress, totalCnt, changed));
					}
				}
				blc = blc.getParent();
				if (blockExist && blc.getBlockHeaderByRawHash(wrapNull(lastScannedBlockHash)) == null) {
					break;
				}
			}
			op = addOpIfNeeded(op, true);
			String lastBlockRawHash = init.getLastBlockRawHash();
			if (changed > 0 &&
					!OUtils.equals(lastBlockRawHash, lastScannedBlockHash)) {
				op = initOpOperation(botObject.getParentType());
				generateSetOperation(op, botObject.getId(),
						F_BOT_STATE + "." + F_BLOCK_HASH, lastScannedBlockHash, lastBlockRawHash);
				addOpIfNeeded(op, true);
			}
			info(String.format("Synchronization of '%s' has finished. Scanned %d, changed %d", botTypeName(),
					progress, changed));
			setSuccessState();
		} catch (Exception e) {
			setFailedState();
			info("Synchronization  of '" + botTypeName() + "' has failed: " + e.getMessage(), e);
			throw e;
		} finally {
			super.shutdown();
		}
		return (T) this;
	}
	


	@Override
	public int total() {
		return totalCnt;
	}

	@Override
	public int progress() {
		return progress;
	}

	private String wrapNull(String tp) {
		if(tp == null) {
			return "";
		}
		return tp;
	}

	public static OpOperation generateSetOperation(OpOperation op, List<String> id, 
			String field, String oldValue, String newValue) {
		OpObject editObject = new OpObject();
		editObject.putObjectValue(F_ID, id);
		Map<String, Object> changeTagMap = new TreeMap<>();
		Map<String, Object> currentTagMap = new TreeMap<>();
		changeTagMap.put(field, set(newValue));
		if(oldValue != null) {
			currentTagMap.put(field, oldValue);
		}
		if (!changeTagMap.isEmpty()) {
			editObject.putObjectValue(F_CHANGE, changeTagMap);
			editObject.putObjectValue(F_CURRENT, currentTagMap);
			op.addEdited(editObject);
		}
		return op;
	}
	
	
	private static Object set(Object vl) {
		Map<String, Object> set = new TreeMap<String, Object>();
		set.put(OpBlockChain.OP_CHANGE_SET, vl);
		return set;
	}



	
}
