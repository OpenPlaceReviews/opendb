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
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.openplacereviews.opendb.ops.OpBlockChain.*;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.LIMIT_FOR_EXTRACTING_OBJECTS;
import static org.openplacereviews.opendb.ops.OpObject.F_FINAL;
import static org.openplacereviews.opendb.ops.OpObject.F_SUBMITTED_OP_HASH;
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
	private TransactionTemplate txTemplate;

	@Autowired
	private JsonFormatter formatter;

	public boolean isRunning() {
		return isRunning;
	}
	
	public void saveHistoryForBlockOperations(OpBlock opBlock, DeletedObjectCtx hctx) {
		if (!isRunning()) {
			return;
		}
		txTemplate.execute(new TransactionCallback<Void>() {

			@Override
			public Void doInTransaction(TransactionStatus status) {
				Date date = new Date(opBlock.getDate(OpBlock.F_DATE));
				for (OpOperation o : opBlock.getOperations()) {
					List<Object[]> allBatches = generateHistoryObjBatch(opBlock, o, date, hctx);
					dbSchema.insertObjIntoHistoryTableBatch(allBatches, OP_OBJ_HISTORY_TABLE, jdbcTemplate);
				}
				return null;
			}
		});
	}

	public void retrieveHistory(HistoryObjectRequest historyObjectRequest) {
		String sql;
		switch (historyObjectRequest.historyType) {
			case HISTORY_BY_USER: {
				StringBuilder userString = getUserSqlRequestString(historyObjectRequest);
				sql = "SELECT usr_1, login_1, usr_2, login_2, p1, p2, p3, p4, p5, time, obj, type, status, ophash FROM " + OP_OBJ_HISTORY_TABLE + " WHERE " +
						userString + " ORDER BY sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
				loadHistory(sql, historyObjectRequest);
				break;
			}
			case HISTORY_BY_OBJECT: {
				String objType = null;
				if (historyObjectRequest.key.size() > 1) {
					objType = historyObjectRequest.key.get(0);
				}
				sql = "SELECT usr_1, login_1, usr_2, login_2, p1, p2, p3, p4, p5, time, obj, type, status, ophash FROM " + OP_OBJ_HISTORY_TABLE + " WHERE " +
						(objType == null ? "" : " type = ? AND ") + dbSchema.generatePKString(OP_OBJ_HISTORY_TABLE, "p%1$d = ?", " AND ",
						(objType == null ? historyObjectRequest.key.size() : historyObjectRequest.key.size() - 1)) +
						" ORDER BY sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
				loadHistory(sql, historyObjectRequest);
				break;
			}
			case HISTORY_BY_TYPE: {
				historyObjectRequest.key = Collections.singletonList(historyObjectRequest.key.get(0));
				sql = "SELECT usr_1, login_1, usr_2, login_2, p1, p2, p3, p4, p5, time, obj, type, status, ophash FROM " + OP_OBJ_HISTORY_TABLE +
						" WHERE type = ?" + " ORDER BY sorder " + historyObjectRequest.sort + " LIMIT " + historyObjectRequest.limit;
				loadHistory(sql, historyObjectRequest);
				break;
			}
		}
	}

	private StringBuilder getUserSqlRequestString(HistoryObjectRequest historyObjectRequest) {
		StringBuilder userString = new StringBuilder();
		int k = 1;
		for (String key : historyObjectRequest.key) {
			if (key.contains(":")) {
				if (userString.length() == 0) {
					userString.append(" usr_").append(k).append(" = ? AND login_").append(k).append(" = ? ");
				} else {
					userString.append(" AND usr_").append(k).append(" = ? AND login_").append(k).append(" = ? ");
				}
			} else {
				if (userString.length() == 0) {
					userString.append(" usr_").append(k).append(" = ? ");
				} else {
					userString.append(" AND usr_").append(k).append(" = ? ");
				}
			}
			k++;
		}
		return userString;
	}

	protected void loadHistory(String sql, HistoryObjectRequest historyObjectRequest) {
		Object[] keyObject = historyObjectRequest.key.toArray();
		keyObject = generateUserSearchObject(historyObjectRequest, keyObject);
		historyObjectRequest.historySearchResult = jdbcTemplate.query(sql, keyObject, new ResultSetExtractor<List<HistoryEdit>>() {
			@Override
			public List<HistoryEdit> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<HistoryEdit> result = new LinkedList<>();

				while (rs.next()) {
					List<String> users = new ArrayList<>();
					String user = "";
					for (int i = 1; i <= 4; i++) {
						if (rs.getString(i) != null) {
							if (user.length() == 0) {
								user = rs.getString(i);
							} else {
								user += ":" + rs.getString(i);
							}
							if (i % 2 == 0) {
								users.add(user);
								user = "";
							}
						}
					}
					List<String> ids = new ArrayList<>();
					ids.add(rs.getString(12));
					for (int i = 5; i <= 4 + MAX_KEY_SIZE; i++) {
						if (rs.getString(i) != null) {
							ids.add(rs.getString(i));
						}
					}
					HistoryEdit historyObject = new HistoryEdit(
							users,
							rs.getString(12),
							formatter.parseObject(rs.getString(11)),
							formatFullDate(rs.getTimestamp(10)),
							HistoryManager.Status.getStatus(rs.getInt(13)),
							SecUtils.hexify(rs.getBytes(14))
					);
					if (historyObject.getStatus().equals(HistoryManager.Status.EDITED)) {
						historyObject.setDeltaChanges(formatter.fromJsonToTreeMap(rs.getString(11)));
					}
					historyObject.setId(ids);
					result.add(historyObject);
				}

				result = generateHistoryObj(result, historyObjectRequest.sort);
				return result;
			}
		});
	}

	private Object[] generateUserSearchObject(HistoryObjectRequest historyObjectRequest, Object[] keyObject) {
		if (HISTORY_BY_USER.equals(historyObjectRequest.historyType)) {
			int loginSize = 0;
			for (String key : historyObjectRequest.key) {
				if (key.contains(":")) {
					loginSize++;
				}
			}
			keyObject = new Object[historyObjectRequest.key.size() + loginSize];

			int i = -1;
			for (String user : historyObjectRequest.key) {
				if (user.contains(":")) {
					String[] args = user.split(":");
					keyObject[++i] = args[0];
					keyObject[++i] = args[1];
				} else {
					if (i == -1) {
						keyObject[0] = user;
						i++;
					} else {
						if (loginSize == 0) {
							keyObject[1] = user;
						} else {
							keyObject[2] = user;
						}
					}
				}
			}
		}
		return keyObject;
	}


	public List<HistoryEdit> generateHistoryObj(List<HistoryEdit> historyList, String sort) {
		List<HistoryEdit> newHistoryList = new ArrayList<>();

		Map<List<String>, HistoryEdit> previousHistoryEditMap = new HashMap<>();
		Map<List<String>, OpObject> originObjectMap = new HashMap<>();
		for (HistoryEdit historyEdit : historyList) {
			OpObject originObject = getPreviousOpObject(originObjectMap.get(historyEdit.id), previousHistoryEditMap.get(historyEdit.id), historyEdit);
			previousHistoryEditMap.put(historyEdit.id, historyEdit);
			newHistoryList.add(historyEdit);
			originObjectMap.put(historyEdit.id, originObject);
		}

		return newHistoryList;
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
			originObject = historyEdit.objEdit;
		} else if (historyEdit.getStatus().equals(Status.EDITED) && originObject == null ||
				historyEdit.getStatus().equals(Status.CREATED) && previousHistoryEdit == null) {
			originObject = blocksManager.getBlockchain().getObjectByName(historyEdit.objType, historyEdit.id.subList(1, historyEdit.id.size()));
			historyEdit.objEdit = originObject;
		} else {
			Map<String, Object> changes = previousHistoryEdit.deltaChanges;
			if (changes == null) {
				historyEdit.objEdit = originObject;
			} else {
				originObject = generateReverseEditObject(originObject, changes);
				if (originObject.getFieldByExpr(OpObject.F_STATE) != null &&
						originObject.getFieldByExpr(OpObject.F_STATE).equals(F_FINAL)) {
					originObject.setFieldByExpr(OpObject.F_STATE, OpObject.F_OPEN);
					originObject.remove(F_SUBMITTED_OP_HASH);
				}
				historyEdit.objEdit = originObject;
			}
		}

		return originObject;
	}

	@SuppressWarnings("unchecked")
	protected OpObject generateReverseEditObject(OpObject originObject, Map<String, Object> changes) {
		Map<String, Object> changeEdit = (Map<String, Object>) changes.get(OpObject.F_CHANGE);
		Map<String, Object> currentEdit = (Map<String, Object>) changes.get(OpObject.F_CURRENT);

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
				Object o = prevObj.getFieldByExpr(fieldExpr);
				if (o instanceof List) {
					List<Object> currentObj = (List<Object>) o;
					Object appendObj = getValueForField(fieldExpr, changeEdit);
					currentObj.remove(appendObj);
					prevObj.setFieldByExpr(fieldExpr, currentObj);
				} else if (o instanceof Map) {
					Map<Object, Object> currentObj = (Map<Object, Object>) o;
					Map<Object, Object> appendObj = (Map<Object, Object>) getValueForField(fieldExpr, changeEdit);
					for (Object key : appendObj.keySet()) {
						currentObj.remove(appendObj.get(key));
					}
					prevObj.setFieldByExpr(fieldExpr, currentObj);
				}
			} else if (OP_CHANGE_SET.equals(opId)) {
				Object previousObject = getValueForField(fieldExpr, currentEdit);
				prevObj.setFieldByExpr(fieldExpr, previousObject);
			} else if (OP_CHANGE_INCREMENT.equals(opId)) {
				Object currentValue = prevObj.getFieldByExpr(fieldExpr);
				prevObj.setFieldByExpr(fieldExpr, ((Number) currentValue).longValue() - 1);
			}

		}

		return prevObj;
	}

	protected Object getValueForField(String fieldExpr, Map<String, Object> editMap) {
		for (Map.Entry<String, Object> e : editMap.entrySet()) {
			if (e.getKey().equals(fieldExpr)) {
				Object op = e.getValue();
				Object opValue = e.getValue();
				if (op instanceof Map) {
					@SuppressWarnings("unchecked")
					Map.Entry<String, Object> ee = ((Map<String, Object>) op).entrySet().iterator().next();
					opValue = ee.getValue();
				}

				return opValue;
			}
		}
		return null;
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
	
	private List<Object[]> generateHistoryObjBatch(OpBlock opBlock, OpOperation op, Date date, DeletedObjectCtx hctx) {
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

	private List<Object[]> prepareArgumentsForHistoryBatch(byte[] blockHash, List<OpObject> opObjectList, OpOperation op, Date date, Status status) {
		List<Object[]> insertBatch = new ArrayList<>(opObjectList.size());

		for (int i = 0; i < opObjectList.size(); i++) {
			Object[] args = new Object[6 + DBSchemaManager.HISTORY_USERS_SIZE * 2 + DBSchemaManager.MAX_KEY_SIZE];
			args[0] = blockHash;
			args[1] = SecUtils.getHashBytes(op.getRawHash());
			args[2] = op.getType();
			args[3] = date;
			args[4] = getObjectByStatus(opObjectList.get(i), status);
			args[5] = status.getValue();
			for(int userInd = 0; userInd < HISTORY_USERS_SIZE; userInd++) {
				putUserKey(6, userInd, args, op.getSignedBy());
			}
			List<String> objIds = opObjectList.get(i).getId();
			if (objIds.isEmpty()) {
				objIds = new ArrayList<>();
				objIds.add(op.getRawHash());
				objIds.add(String.valueOf(i));
			}
			int k = 6 + DBSchemaManager.HISTORY_USERS_SIZE * 2;
			for (String id : objIds) {
				args[k] = id;
				k++;
			}

			insertBatch.add(args);
		}

		return insertBatch;
	}

	private void putUserKey(int arrInd, int userInd, Object[] args, List<String> ls) {
		if(ls.size() > userInd) {
			String[] un = ls.get(userInd).split(":");
			args[arrInd + userInd * 2] = un[0];
			if(un.length > 1) {
				args[arrInd + userInd * 2 + 1] = un[1];
			}
		}
	}

	public static class HistoryObjectRequest {
		public String historyType;
		public List<String> key;
		public int limit = LIMIT_FOR_EXTRACTING_OBJECTS;
		public String sort;
		public List<HistoryEdit> historySearchResult;

		public HistoryObjectRequest(String historyType, List<String> key, int limit, String sort) {
			if (limit < LIMIT_FOR_EXTRACTING_OBJECTS) {
				this.limit = limit;
			}
			this.historyType = historyType;
			this.key = key;
			this.sort = sort;
		}
	}

	public static class HistoryEdit {
		private List<String> id;
		private List<String> userId;
		private String objType;
		private OpObject objEdit;
		private TreeMap<String, Object> deltaChanges;
		private String date;
		private Status status;
		private String opHash;

		public HistoryEdit(List<String> userId, String objType, OpObject objEdit, String date, Status status, String ophash) {
			this.userId = userId;
			this.objType = objType;
			this.objEdit = objEdit;
			this.date = date;
			this.status = status;
			this.opHash = ophash;
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

		public OpObject getObjEdit() {
			return objEdit;
		}

		public void setObjEdit(OpObject objEdit) {
			this.objEdit = objEdit;
		}

		public TreeMap<String, Object> getDeltaChanges() {
			return deltaChanges;
		}

		public void setDeltaChanges(TreeMap<String, Object> deltaChanges) {
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
