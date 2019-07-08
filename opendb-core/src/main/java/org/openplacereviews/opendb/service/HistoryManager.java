package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpBlockChain.*;
import static org.openplacereviews.opendb.service.DBSchemaManager.HISTORY_TABLE_SIZE;
import static org.openplacereviews.opendb.service.DBSchemaManager.USER_KEY_SIZE;

@Service
public class HistoryManager {

	private static final String ASC_SORT = "ASC";
	private static final String DESC_SORT = "DESC";

	@Autowired
	private DBConsensusManager dataManager;

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private JsonFormatter formatter;

	public void saveHistoryForBlockOperations(OpBlock opBlock,  Map<String, OpObject> deletedObjs) {
		Date date = new Date(opBlock.getDate(OpBlock.F_DATE));
		for (OpOperation o : opBlock.getOperations()) {
			List<Object[]> allBatches = generateHistoryObjBatch(opBlock, o, date, deletedObjs);
			dataManager.saveHistoryForOperationObjects(allBatches);
		}
	}

	public Map<List<String>, List<HistoryEdit>> generateHistoryObj(Map<List<String>, List<HistoryEdit>> historyMap, String sort) {
		Map<List<String>, List<HistoryEdit>> newHistoryMap = new LinkedHashMap<>();

		for(List<String> keys : historyMap.keySet()) {
			List<HistoryEdit> loadedList = historyMap.get(keys);
			List<HistoryEdit> newHistoryList = new LinkedList<>();
			OpObject originObject = null;
			HistoryEdit previousHistoryEdit = null;
			if (sort.equals(ASC_SORT)) {
				Collections.reverse(loadedList);
				for (HistoryEdit historyEdit : loadedList) {
					originObject = getPreviousOpObject(originObject, previousHistoryEdit, historyEdit);
					previousHistoryEdit = historyEdit;
					newHistoryList.add(historyEdit);
				}
				Collections.reverse(newHistoryList);
			} else if (sort.equals(DESC_SORT)){
				for (HistoryEdit historyEdit : loadedList) {
					originObject = getPreviousOpObject(originObject, previousHistoryEdit, historyEdit);
					previousHistoryEdit = historyEdit;
					newHistoryList.add(historyEdit);
				}
			}
			newHistoryMap.put(keys, newHistoryList);
		}

		return newHistoryMap;
	}

	private OpObject getPreviousOpObject(OpObject originObject, HistoryEdit previousHistoryEdit, HistoryEdit historyEdit) {
		if (historyEdit.getStatus().equals(Status.DELETED)) {
			originObject = historyEdit.deltaChanges;
		} else if (historyEdit.getStatus().equals(Status.EDITED) && originObject == null ||
				historyEdit.getStatus().equals(Status.CREATED) && previousHistoryEdit == null) {
			originObject = blocksManager.getBlockchain().getObjectByName(historyEdit.objType, historyEdit.id);
			historyEdit.deltaChanges = originObject;
		} else {
			Map<String, Object> changes = previousHistoryEdit.objEdit;
			if (changes == null) {
				historyEdit.deltaChanges = originObject;
			} else {
				originObject = generateReverseEditObject(originObject, changes);
				historyEdit.deltaChanges = originObject;
			}
		}

		return originObject;
	}

	private OpObject generateReverseEditObject(OpObject originObject, Map<String, Object> changes) {
		TreeMap<String, Object> changeEdit = (TreeMap<String, Object>) changes.get(OpObject.F_CHANGE);
		TreeMap<String, Object> currentEdit = (TreeMap<String, Object>) changes.get(OpObject.F_CURRENT);

		OpObject prevObj = new OpObject(originObject);
		for (Map.Entry<String, Object> e : changeEdit.entrySet()) {
			String fieldExpr = e.getKey();
			Object op = e.getValue();
			String opId = op.toString();
			Object opValue = getPreviousValueForField(fieldExpr, currentEdit);
			if (op instanceof Map) {
				Map.Entry<String, Object> ee = ((Map<String, Object>) op).entrySet().iterator().next();
				opId = ee.getKey();
				opValue = getPreviousValueForField(fieldExpr, currentEdit);
			}

			if (OP_CHANGE_DELETE.equals(opId)) {
				prevObj.setFieldByExpr(fieldExpr, opValue);
			} else if (OP_CHANGE_APPEND.equals(opId)) {
				List<Object> args = new ArrayList<>(1);
				args.add(opValue);
				prevObj.setFieldByExpr(fieldExpr, args);
			} else if (OP_CHANGE_SET.equals(opId)) {
				prevObj.setFieldByExpr(fieldExpr, null);
			} else if (OP_CHANGE_INCREMENT.equals(opId)) {
				prevObj.setFieldByExpr(fieldExpr, opValue);
			}

		}

		return prevObj;
	}

	private Object getPreviousValueForField(String fieldExpr, TreeMap<String, Object> currentValues) {
		for (Map.Entry<String, Object> e : currentValues.entrySet()) {
			if (e.getKey().equals(fieldExpr)) {
				Object op = e.getValue();
				Object opValue = e.getValue();
				if (op instanceof Map) {
					Map.Entry<String, Object> ee = ((Map<String, Object>) op).entrySet().iterator().next();
					opValue = ee.getValue();
				}

				return opValue;
			}
		}

		return null;
	}

	protected List<Object[]> generateHistoryObjBatch(OpBlock opBlock, OpOperation op, Date date, Map<String, OpObject> deletedObjs) {
		byte[] blockHash = SecUtils.getHashBytes(opBlock.getFullHash());
		List<Object[]> args = new LinkedList<>();

		args.addAll(prepareArgumentsForHistoryBatch(blockHash, op.getCreated(), op, date, Status.CREATED));
		for (String key : deletedObjs.keySet()) {
			if (key.equals(op.getHash())) {
				args.addAll(prepareArgumentsForHistoryBatch(blockHash, deletedObjs.values(), op, date, Status.DELETED));
			}
		}
		args.addAll(prepareArgumentsForHistoryBatch(blockHash, op.getEdited(), op, date, Status.EDITED));

		return args;
	}

	private Object getObjectByStatus(OpObject opObject, Status status) {
		if (status.equals(Status.CREATED)) {
			return null;
		} else if (status.equals(Status.EDITED)) {
			Map<String, Object> editList = new LinkedHashMap<>();
			editList.put(OpObject.F_CHANGE, opObject.getChangedEditFields());
			editList.put(OpObject.F_CURRENT, opObject.getCurrentEditFields());

			PGobject obj = new PGobject();
			obj.setType("jsonb");
			try {
				obj.setValue(formatter.fullObjectToJson(editList));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return obj;
		} else if (status.equals(Status.DELETED)) {
			PGobject obj = new PGobject();
			obj.setType("jsonb");
			try {
				obj.setValue(formatter.fullObjectToJson(opObject));
			} catch (SQLException e) {
				e.printStackTrace();
			}

			return obj;
		}

		return null;
	}

	private List<Object[]> prepareArgumentsForHistoryBatch(byte[] blockHash, List<OpObject> opObjectList, OpOperation op, Date date, Status status) {
		List<Object[]> insertBatch = new ArrayList<>(opObjectList.size());

		for (int i = 0; i < opObjectList.size(); i++) {
			Object[] args = new Object[HISTORY_TABLE_SIZE];
			args[0] = blockHash;
			args[1] = SecUtils.getHashBytes(op.getRawHash());
			args[2] = op.getType();
			args[3] = date;
			args[4] = getObjectByStatus(opObjectList.get(i), status);
			args[5] = status.getValue();
			generateSignedByArguments(args, op.getSignedBy(), 6);

			List<String> objIds = opObjectList.get(i).getId();
			if (objIds.isEmpty()) {
				objIds = new ArrayList<>();
				objIds.add(op.getRawHash());
				objIds.add(String.valueOf(i));
			}

			int k = 8;
			for (String id : objIds) {
				args[k] = id;
				k++;
			}

			insertBatch.add(args);
		}

		return insertBatch;
	}

	private List<Object[]> prepareArgumentsForHistoryBatch(byte[] blockHash, Collection<OpObject> deletedObjects, OpOperation op, Date date, Status status) {
		List<Object[]> insertBatch = new ArrayList<>(deletedObjects.size());

		for (OpObject opObject : deletedObjects) {
			Object[] args = new Object[HISTORY_TABLE_SIZE];
			args[0] = blockHash;
			args[1] = SecUtils.getHashBytes(op.getHash());
			args[2] = op.getType();
			args[3] = date;
			args[4] = getObjectByStatus(opObject, status);
			args[5] = status.getValue();
			generateSignedByArguments(args, op.getSignedBy(), 6);

			int k = 8;
			for (String id : opObject.getId()) {
				args[k] = id;
				k++;
			}
			insertBatch.add(args);
		}

		return insertBatch;
	}

	private void generateSignedByArguments(Object[] args, List<String> signedBy, int k) {
		for (int i = 0; i < USER_KEY_SIZE; i++) {
			if (signedBy.size() > 1) {
				args[k] = signedBy.get(i);
			} else {
				if (k == 7) {
					args[k] = null;
				} else {
					args[k] = signedBy.get(0);
				}
			}
			k++;
		}
	}

	public static class HistoryObjectRequest {
		public String historyType;
		public List<String> key;
		public int limit;
		public String sort;
		public Map<List<String>, List<HistoryManager.HistoryEdit>> historySearchResult;

		public HistoryObjectRequest(String historyType, List<String> key, int limit, String sort) {
			this.historyType = historyType;
			this.key = key;
			this.limit = limit;
			this.sort = sort;
		}
	}

	public static class HistoryEdit {
		private List<String> id;
		private List<String> userId;
		private String objType;
		private TreeMap<String, Object> objEdit;
		private OpObject deltaChanges;
		private String date;
		private Status status;

		public HistoryEdit() {
		}

		public HistoryEdit(List<String> userId, String objType, OpObject deltaChanges, String date, Status status) {
			this.userId = userId;
			this.objType = objType;
			this.deltaChanges = deltaChanges;
			this.date = date;
			this.status = status;
		}

		public List<String> getId() {
			return id;
		}

		public void setId(List<String> id) {
			this.id = id;
		}

		public List<String> getUserId() {
			return userId;
		}

		public void setUserId(List<String> userId) {
			this.userId = userId;
		}

		public String getObjType() {
			return objType;
		}

		public void setObjType(String objType) {
			this.objType = objType;
		}

		public TreeMap<String, Object> getObjEdit() {
			return objEdit;
		}

		public void setObjEdit(TreeMap<String, Object> objEdit) {
			this.objEdit = objEdit;
		}

		public OpObject getDeltaChanges() {
			return deltaChanges;
		}

		public void setDeltaChanges(OpObject deltaChanges) {
			this.deltaChanges = deltaChanges;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public Status getStatus() {
			return status;
		}

		public void setStatus(Status status) {
			this.status = status;
		}
	}

	public enum Status {
		CREATED(0),
		DELETED(1),
		EDITED(2),
		NOT_SPECIFIED(3);

		private final int value;

		Status(final int newValue) {
			value = newValue;
		}

		public int getValue() {
			return value;
		}

		public static Status getStatus(int value) {
			switch (value) {
				case 0: {
					return CREATED;
				}
				case 1: {
					return DELETED;
				}
				case 2: {
					return EDITED;
				}
				default: {
					return NOT_SPECIFIED;
				}
			}
		}
	}
}
