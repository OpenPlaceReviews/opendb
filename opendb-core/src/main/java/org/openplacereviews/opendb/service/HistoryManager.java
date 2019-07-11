package org.openplacereviews.opendb.service;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpBlockChain.*;
import static org.openplacereviews.opendb.ops.OpObject.F_FINAL;
import static org.openplacereviews.opendb.service.DBSchemaManager.*;

@Service
public class HistoryManager {

	protected static final String ASC_SORT = "ASC";
	protected static final String DESC_SORT = "DESC";

	protected static final String HISTORY_BY_USER = "user";
	protected static final String HISTORY_BY_OBJECT = "object";
	protected static final String HISTORY_BY_TYPE = "type";
	
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(OpObject.DATE_FORMAT);

	@Value("${opendb.db.store-history}")
	private boolean isRunning;

	@Autowired
	private DBSchemaManager dbSchema;

	@Autowired
	private BlocksManager blocksManager;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private JsonFormatter formatter;

	public boolean isRunning() {
		return isRunning;
	}

	public void retrieveHistory(HistoryObjectRequest historyObjectRequest) {
		String sql;
		switch (historyObjectRequest.historyType) {
			case HISTORY_BY_USER: {
				sql = "SELECT u1, u2, p1, p2, p3, p4, p5, time, obj, type, status FROM " + OP_OBJ_HISTORY_TABLE + " WHERE " +
						dbSchema.generatePKString(OP_OBJ_HISTORY_TABLE, "u%1$d = ?", " AND ", historyObjectRequest.key.size()) +
						" ORDER BY sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
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
						" ORDER BY sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
				loadHistory(sql, historyObjectRequest);
				break;
			}
			case HISTORY_BY_TYPE: {
				historyObjectRequest.key = Collections.singletonList(historyObjectRequest.key.get(0));
				sql = "SELECT u1, u2, p1, p2, p3, p4, p5, time, obj, type, status FROM " + OP_OBJ_HISTORY_TABLE +
						" WHERE type = ?" + " ORDER BY sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
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

				List<List<String>> objIds = new LinkedList<>();
				List<HistoryEdit> allObjects = new LinkedList<>();
				while (rs.next()) {
					List<String> users = new ArrayList<>();
					for (int i = 1; i <= USER_KEY_SIZE; i++) {
						if (rs.getString(i) != null) {
							users.add(rs.getString(i));
						}
					}
					List<String> ids = new ArrayList<>();
					ids.add(rs.getString(10));
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

				result = generateHistoryObj(result, historyObjectRequest.sort);
				return result;
			}
		});
	}

	public void saveHistoryForBlockOperations(OpBlock opBlock, HistoryObjectCtx hctx) {
		Date date = new Date(opBlock.getDate(OpBlock.F_DATE));
		for (OpOperation o : opBlock.getOperations()) {
			List<Object[]> allBatches = generateHistoryObjBatch(opBlock, o, date, hctx);
			saveHistoryForOperationObjects(allBatches);
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

	private void saveHistoryForOperationObjects(List<Object[]> allBatches) {
		dbSchema.insertObjIntoHistoryTableBatch(allBatches, OP_OBJ_HISTORY_TABLE, jdbcTemplate);
	}

	private String formatFullDate(Date date) {
		if (date == null)
			return null;

		return DATE_FORMAT.format(date);
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

	protected OpObject getPreviousOpObject(OpObject originObject, HistoryEdit previousHistoryEdit, HistoryEdit historyEdit) {
		if (historyEdit.getStatus().equals(Status.DELETED)) {
			originObject = historyEdit.deltaChanges;
		} else if (historyEdit.getStatus().equals(Status.EDITED) && originObject == null ||
				historyEdit.getStatus().equals(Status.CREATED) && previousHistoryEdit == null) {
			originObject = blocksManager.getBlockchain().getObjectByName(historyEdit.objType, historyEdit.id.subList(1, historyEdit.id.size()));
			historyEdit.deltaChanges = originObject;
		} else {
			Map<String, Object> changes = previousHistoryEdit.objEdit;
			if (changes == null) {
				historyEdit.deltaChanges = originObject;
			} else {
				originObject = generateReverseEditObject(originObject, changes);
				if (originObject.getFieldByExpr(OpObject.F_STATE) != null &&
						originObject.getFieldByExpr(OpObject.F_STATE).equals(F_FINAL)) {
					originObject.setFieldByExpr(OpObject.F_STATE, OpObject.F_OPEN);
				}
				historyEdit.deltaChanges = originObject;
			}
		}

		return originObject;
	}

	protected OpObject generateReverseEditObject(OpObject originObject, Map<String, Object> changes) {
		TreeMap<String, Object> changeEdit = (TreeMap<String, Object>) changes.get(OpObject.F_CHANGE);
		TreeMap<String, Object> currentEdit = (TreeMap<String, Object>) changes.get(OpObject.F_CURRENT);

		OpObject prevObj = new OpObject(originObject);
		for (Map.Entry<String, Object> e : changeEdit.entrySet()) {
			String fieldExpr = e.getKey();
			Object op = e.getValue();
			String opId = op.toString();
			if (op instanceof Map) {
				Map.Entry<String, Object> ee = ((Map<String, Object>) op).entrySet().iterator().next();
				opId = ee.getKey();
			}

			if (OP_CHANGE_DELETE.equals(opId)) {
				Object previousObj = getValueForField(fieldExpr, currentEdit);
				prevObj.setFieldByExpr(fieldExpr, previousObj);
			} else if (OP_CHANGE_APPEND.equals(opId)) {
				List<Object> currentObj = (List<Object>) prevObj.getFieldByExpr(fieldExpr);
				Object appendObj = getValueForField(fieldExpr, changeEdit);
				currentObj.remove(appendObj);
				prevObj.setFieldByExpr(fieldExpr, currentObj);
			} else if (OP_CHANGE_SET.equals(opId)) {
				prevObj.setFieldByExpr(fieldExpr, null);
			} else if (OP_CHANGE_INCREMENT.equals(opId)) {
				Object currentValue = prevObj.getFieldByExpr(fieldExpr);
				prevObj.setFieldByExpr(fieldExpr, ((Number) currentValue).longValue() - 1);
			}

		}

		return prevObj;
	}

	protected Object getValueForField(String fieldExpr, TreeMap<String, Object> editMap) {
		for (Map.Entry<String, Object> e : editMap.entrySet()) {
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

	protected List<Object[]> generateHistoryObjBatch(OpBlock opBlock, OpOperation op, Date date, HistoryObjectCtx hctx) {
		byte[] blockHash = SecUtils.getHashBytes(opBlock.getFullHash());
		List<Object[]> args = new LinkedList<>();

		args.addAll(prepareArgumentsForHistoryBatch(blockHash, op.getCreated(), op, date, Status.CREATED));
		if (hctx != null) {
			for (String key : hctx.deletedObjsCache.keySet()) {
				if (key.equals(op.getHash())) {
					args.addAll(prepareArgumentsForHistoryBatch(blockHash, hctx.deletedObjsCache.get(key), op, date, Status.DELETED));
				}
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

	private void generateSignedByArguments(Object[] args, List<String> signedBy, int k) {
		if (signedBy.size() > 1) {
			args[k] = signedBy.toArray();
		} else {
			args[k] = signedBy.get(0);
		}
	}

	public static class HistoryObjectCtx {
		final String blockHash;
		public Map<String, List<OpObject>> deletedObjsCache = new LinkedHashMap<>();

		public HistoryObjectCtx(String bhash) {
			blockHash = bhash;
		}

		public void putObjectToDeleteCache(String key, OpObject opObject) {
			List<OpObject> list = deletedObjsCache.get(key);
			if (list == null) {
				list = new ArrayList<>();
			}
			list.add(opObject);
			deletedObjsCache.put(key, list);
		}
	}

	public static class HistoryObjectRequest {
		public String historyType;
		public List<String> key;
		public int limit;
		public String sort;
		public Map<List<String>, List<HistoryEdit>> historySearchResult;

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
