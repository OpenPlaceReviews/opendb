package org.openplacereviews.opendb.service;

import org.junit.After;
import org.junit.Test;
import org.openplacereviews.opendb.ops.OpObject;

import java.util.*;

import static org.junit.Assert.*;

public class HistoryManagerTest {

	private HistoryManager historyManager = new HistoryManager();

	@After
	public void tearDown() {
	}

	@Test
	public void testGetPreviousOpObject() {
		OpObject originObject = null;
		HistoryManager.HistoryEdit previousHistoryEdit = null;
		HistoryManager.HistoryEdit historyEdit = new HistoryManager.HistoryEdit(
				Arrays.asList("user", "test1"),
				"sys.operation",
				generateTestDeletedDeltaObject(),
				new Date().toString(),
				HistoryManager.Status.DELETED
		);

		originObject = historyManager.getPreviousOpObject(originObject, previousHistoryEdit, historyEdit);
		previousHistoryEdit = historyEdit;
		assertNotNull(originObject);

		HistoryManager.HistoryEdit firstHistoryEdit = generateHistoryEdit(
				generateTestDeletedDeltaObject(),
				HistoryManager.Status.EDITED,
				null
		);

		firstHistoryEdit.setObjEdit(generateObjEdit());
		originObject = historyManager.getPreviousOpObject(originObject, previousHistoryEdit, firstHistoryEdit);
		previousHistoryEdit = firstHistoryEdit;

		assertNull(originObject.getFieldByExpr("lat"));
		assertEquals(originObject.getFieldByExpr("lon"), 12346);
		assertEquals(originObject.getFieldByExpr("addf"), 1234567);
		assertEquals(((List)originObject.getFieldByExpr("tags")).size(), 3);

		HistoryManager.HistoryEdit secondHistoryEdit = generateHistoryEdit(
				generateTestDeletedDeltaObject(),
				HistoryManager.Status.EDITED,
				generateSecondObjEdit()
		);
		secondHistoryEdit.setObjEdit(generateSecondObjEdit());
		originObject = historyManager.getPreviousOpObject(originObject, previousHistoryEdit, secondHistoryEdit);
		previousHistoryEdit = secondHistoryEdit;

		assertEquals(originObject.getFieldByExpr("lon"), 12345L);
		assertEquals(originObject.getFieldByExpr("lat"), "222EC");
		assertNull(originObject.getFieldByExpr("addf"));
		assertEquals(((List)originObject.getFieldByExpr("tags")).size(), 2);


		HistoryManager.HistoryEdit createHistoryEdit = generateHistoryEdit(
				null,
				HistoryManager.Status.CREATED,
				null
		);
		originObject = historyManager.getPreviousOpObject(originObject, previousHistoryEdit, createHistoryEdit);
		assertEquals(originObject.getFieldByExpr("lon"), 12344L);
		assertEquals(originObject.getFieldByExpr("lat"), "222EC");
		assertEquals(originObject.getFieldByExpr("det"), "111EC");
		assertEquals(((List)originObject.getFieldByExpr("tags")).size(), 1);
	}

	private HistoryManager.HistoryEdit generateHistoryEdit(OpObject delataChanges, HistoryManager.Status status, TreeMap<String, Object> objEdit) {
		HistoryManager.HistoryEdit historyEdit = new HistoryManager.HistoryEdit(
				Arrays.asList("user", "test1"),
				"sys.operation",
				delataChanges,
				new Date().toString(),
				status
		);
		historyEdit.setObjEdit(objEdit);

		return historyEdit;
	}

	private TreeMap<String, Object> generateObjEdit() {
		TreeMap<String, Object> editObj = new TreeMap<>();
		TreeMap<String, Object> changeObject = new TreeMap<>();
		TreeMap<String, Object> currentObject = new TreeMap<>();

		TreeMap<String, Object> setObj = new TreeMap<>();
		setObj.put("set", 1234567);
		changeObject.put("lat", "delete");
		changeObject.put("addf", setObj);
		changeObject.put("lon", "increment");
		Map<String, Map<String, Object>> tagObject = new TreeMap<>();
		Map<String, Object> subTagObject = new TreeMap<>();
		subTagObject.put("k", 333333333);
		subTagObject.put("v", 222222222);
		tagObject.put("append", subTagObject);
		changeObject.put("tags", tagObject);

		currentObject.put("lat", "222EC");

		editObj.put(OpObject.F_CHANGE, changeObject);
		editObj.put(OpObject.F_CURRENT, currentObject);

		return editObj;
	}

	private  TreeMap<String, Object> generateSecondObjEdit() {
		TreeMap<String, Object> editObj = new TreeMap<>();
		TreeMap<String, Object> changeObject = new TreeMap<>();
		TreeMap<String, Object> currentObject = new TreeMap<>();

		changeObject.put("det", "delete");
		changeObject.put("lon", "increment");
		Map<String, Map<String, Object>> tagObject = new TreeMap<>();
		Map<String, Object> subTagObject = new TreeMap<>();
		subTagObject.put("k", 122233333);
		subTagObject.put("v", 222234445);
		tagObject.put("append", subTagObject);
		changeObject.put("tags", tagObject);

		currentObject.put("det", "111EC");

		editObj.put(OpObject.F_CHANGE, changeObject);
		editObj.put(OpObject.F_CURRENT, currentObject);

		return editObj;
	}

	private OpObject generateTestDeletedDeltaObject() {
		OpObject opObject = new OpObject();
		opObject.setId("test", "test2");
		opObject.putObjectValue("lon", 12346);
		opObject.putObjectValue("addf", 1234567);

		List<Object> listObj = new ArrayList<>(2);
		Map<String, Object> mapTagObj = new LinkedHashMap<>();
		mapTagObj.put("k", 2222222);
		mapTagObj.put("v", 1111111);
		listObj.add(mapTagObj);
		Map<String, Object> mapTagObj1 = new LinkedHashMap<>();
		mapTagObj1.put("k", 333333333);
		mapTagObj1.put("v", 222222222);
		listObj.add(mapTagObj1);
		Map<String, Object> mapTagObj2 = new LinkedHashMap<>();
		mapTagObj2.put("k", 122233333);
		mapTagObj2.put("v", 222234445);
		listObj.add(mapTagObj2);
		opObject.putObjectValue("tags", listObj);

		return opObject;
	}

}
