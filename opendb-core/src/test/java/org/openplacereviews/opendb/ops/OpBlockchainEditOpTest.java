package org.openplacereviews.opendb.ops;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.openplacereviews.opendb.ops.OpBlockchainRules.BlockchainValidationException;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.Assert.*;
import static org.openplacereviews.opendb.ObjectGeneratorTest.addOperationFromList;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateUserOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_OPERATION;

public class OpBlockchainEditOpTest {

	private static final String OBJ_ID = "123456678";
	private static final String OP_ID = "osm.testplace";

	public OpBlockChain blc;

	@Spy
	public JsonFormatter jsonFormatter;

	@Spy
	public BlocksManager blocksManager;

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		MockitoAnnotations.initMocks(this);

		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(jsonFormatter, null));
		blocksManager.init(null, blc);
		generateUserOperations(jsonFormatter, blc);
		for (OpOperation opOperation : generateStartOpForTest()) {
			opOperation.makeImmutable();
			blc.addOperation(opOperation);
		}

		ReflectionTestUtils.setField(blocksManager, "serverUser", serverName);
		ReflectionTestUtils.setField(blocksManager, "serverKeyPair", serverKeyPair);
	}

	@Test
	public void testEditAppendOp() throws FailedVerificationException {
		OpOperation editOp = new OpOperation();
		editOp.setType(OP_ID);
		editOp.setSignedBy(serverName);

		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		TreeMap<String, Object> appendObj = new TreeMap<>();
		appendObj.put("append", Arrays.asList("323232"));
		TreeMap<String, Object> secondAppendObj = new TreeMap<>();
		TreeMap<String, Object> secondAppendSubMapObj = new TreeMap<>();
		secondAppendSubMapObj.put("v", 32423423);
		secondAppendObj.put("append", secondAppendSubMapObj);
		changed.put("tags.v", appendObj);
		changed.put("tags.k", secondAppendObj);

		editObj.putObjectValue(OpObject.F_CHANGE, changed);

		// current values for append are not needed
		editObj.putObjectValue(OpObject.F_CURRENT, current);
		editOp.addEdited(editObj);
		blc.getRules().generateHashAndSign(editOp, serverKeyPair);
		editOp.makeImmutable();
		blc.addOperation(editOp);

		OpObject opObject = blc.getObjectByName(OP_ID, OBJ_ID);
		assertEquals("[23423423423, [323232]]", String.valueOf(opObject.getFieldByExpr("tags.v")));
		assertEquals("{v=32423423}", String.valueOf(opObject.getFieldByExpr("tags.k")));
	}

	@Test
	public void testEditIncrementOp() throws FailedVerificationException {
		OpOperation editOp = new OpOperation();
		editOp.setType(OP_ID);
		editOp.setSignedBy(serverName);

		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		changed.put("lon.k", "increment");
		changed.put("def", "increment");

		editObj.putObjectValue(OpObject.F_CHANGE, changed);

		// current values for increment are not needed
		editObj.putObjectValue(OpObject.F_CURRENT, current);
		editOp.addEdited(editObj);
		blc.getRules().generateHashAndSign(editOp, serverKeyPair);
		editOp.makeImmutable();
		blc.addOperation(editOp);

		OpObject opObject = blc.getObjectByName(OP_ID, OBJ_ID);
		assertEquals(123457L, opObject.getFieldByExpr("lon.k"));
		assertEquals(23457L, opObject.getFieldByExpr("def"));
	}

	@Test
	public void testEditDeleteOp() throws FailedVerificationException {
		OpOperation editOp = new OpOperation();
		editOp.setType(OP_ID);
		editOp.setSignedBy(serverName);

		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		changed.put("lon.k", "delete");
		changed.put("def", "delete");
		changed.put("lat", "delete");

		editObj.putObjectValue(OpObject.F_CHANGE, changed);

		// need set current values for op delete
		TreeMap<String, Object> lonObject = new TreeMap<>();
		lonObject.put("k", 123456);
		current.put("lon.k", 123456);
		current.put("def", 23456);
		current.put("lat", "222EC");
		editObj.putObjectValue(OpObject.F_CURRENT, current);

		editOp.addEdited(editObj);
		blc.getRules().generateHashAndSign(editOp, serverKeyPair);
		editOp.makeImmutable();
		blc.addOperation(editOp);

		OpObject opObject = blc.getObjectByName(OP_ID, OBJ_ID);
		assertNull(opObject.getFieldByExpr("lon.k"));
		assertNull(opObject.getFieldByExpr("def"));
		assertNull(opObject.getFieldByExpr("lat"));
	}

	@Test(expected = BlockchainValidationException.class)
	public void testEditDeleteOpWithoutCurrentField() throws FailedVerificationException {
		OpOperation editOp = new OpOperation();
		editOp.setType(OP_ID);
		editOp.setSignedBy(serverName);

		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		changed.put("lon.k", "delete");
		changed.put("def", "delete");
		changed.put("lat", "delete");

		editObj.putObjectValue(OpObject.F_CHANGE, changed);

		editObj.putObjectValue(OpObject.F_CURRENT, current);

		editOp.addEdited(editObj);
		blc.getRules().generateHashAndSign(editOp, serverKeyPair);
		editOp.makeImmutable();

		blc.addOperation(editOp);
	}

	@Test
	public void testEditSetOp() throws FailedVerificationException {
		OpOperation editOp = new OpOperation();
		editOp.setType(OP_ID);
		editOp.setSignedBy(serverName);

		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		TreeMap<String, Object> setObj = new TreeMap<>();
		setObj.put("set", "Mark");
		TreeMap<String, Object> setAlreadyExist = new TreeMap<>();
		setAlreadyExist.put("set", 123);
		changed.put("cat", setObj);
		changed.put("def", setAlreadyExist);

		editObj.putObjectValue(OpObject.F_CHANGE, changed);

		// need set current values only for existed fields
		current.put("def", 23456);
		editObj.putObjectValue(OpObject.F_CURRENT, current);

		editOp.addEdited(editObj);
		blc.getRules().generateHashAndSign(editOp, serverKeyPair);
		editOp.makeImmutable();
		blc.addOperation(editOp);

		OpObject opObject = blc.getObjectByName(OP_ID, OBJ_ID);
		assertEquals("Mark", opObject.getFieldByExpr("cat"));
		assertEquals(123, opObject.getFieldByExpr("def"));
	}

	@Test(expected = BlockchainValidationException.class)
	public void testEditSetOpWithoutExistedCurrentValue() throws FailedVerificationException {
		OpOperation editOp = new OpOperation();
		editOp.setType(OP_ID);
		editOp.setSignedBy(serverName);

		OpObject editObj = new OpObject();
		editObj.setId(OBJ_ID);
		TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		TreeMap<String, Object> setObj = new TreeMap<>();
		setObj.put("set", "Mark");
		TreeMap<String, Object> setAlreadyExist = new TreeMap<>();
		setAlreadyExist.put("set", 123);
		changed.put("cat", setObj);
		changed.put("def", setAlreadyExist);

		editObj.putObjectValue(OpObject.F_CHANGE, changed);
		editObj.putObjectValue(OpObject.F_CURRENT, current);

		editOp.addEdited(editObj);
		blc.getRules().generateHashAndSign(editOp, serverKeyPair);
		editOp.makeImmutable();
		blc.addOperation(editOp);
	}

	@Test
	public void testEditImagesOp() throws FailedVerificationException {
		OpOperation opOperation = new OpOperation();
		opOperation.setType("osm.testplace");
		opOperation.setSignedBy(serverName);
		List<Object> edits = new ArrayList<>();
		OpObject edit = new OpObject();
		edit.setId(OBJ_ID);
		List<Object> imageResponseList = new ArrayList<>();
		Map<String, Object> imageMap = new TreeMap<>();
		imageMap.put("cid", "__OPRImage.cid");
		imageMap.put("hash", "__OPRImage.hash");
		imageMap.put("extension", "__OPRImage.extension");
		imageMap.put("type", "__OPRImage.type");
		imageResponseList.add(imageMap);
		List<String> ids = new ArrayList<>(Arrays.asList("__placeId"));
		Map<String, Object> change = new TreeMap<>();
		Map<String, Object> images = new TreeMap<>();
		Map<String, Object> outdoor = new TreeMap<>();
		outdoor.put("outdoor", imageResponseList);
		images.put("append", outdoor);
		change.put("version", "increment");
		change.put("images", images);
		TreeMap<String, Object> setAlreadyExist = new TreeMap<>();
		setAlreadyExist.put("images", new ArrayList<>());
		TreeMap<String, Object> current = new TreeMap<>();
		edit.putObjectValue(OpObject.F_CHANGE, change);
		edit.putObjectValue(OpObject.F_CURRENT, current);
		edits.add(edit);
		opOperation.putObjectValue(OpOperation.F_EDIT, edit);
		opOperation.addEdited(edit);
		blc.getRules().generateHashAndSign(opOperation, serverKeyPair);
		opOperation.makeImmutable();
		assertTrue(blc.addOperation(opOperation));
	}

	private List<OpOperation> generateStartOpForTest() throws FailedVerificationException {
		OpOperation initOp = new OpOperation();
		initOp.setType(OP_OPERATION);
		initOp.setSignedBy(serverName);
		OpObject createObj = new OpObject();
		createObj.setId(OP_ID);
		initOp.addCreated(createObj);
		blc.getRules().generateHashAndSign(initOp, serverKeyPair);

		OpOperation newOpObject = new OpOperation();
		newOpObject.setType(OP_ID);
		newOpObject.setSignedBy(serverName);
		OpObject createObjForNewOpObject = new OpObject();
		createObjForNewOpObject.setId(OBJ_ID);
		TreeMap<String, Object> lonObject = new TreeMap<>();
		lonObject.put("k", 123456);
		createObjForNewOpObject.putObjectValue("lon", lonObject);
		createObjForNewOpObject.putObjectValue("def", 23456);
		createObjForNewOpObject.putObjectValue("lat", "222EC");
		TreeMap<String, Object> tagsObject = new TreeMap<>();
		tagsObject.put("v", Arrays.asList("23423423423"));
		tagsObject.put("k", Collections.emptyMap());
		createObjForNewOpObject.putObjectValue("tags", tagsObject);

		newOpObject.addCreated(createObjForNewOpObject);
		blc.getRules().generateHashAndSign(newOpObject, serverKeyPair);

		return Arrays.asList(initOp, newOpObject);
	}

	@Test
	public void testMergeOpOsmOrder() throws FailedVerificationException {
		blc.addOperation(createMergeOperation());

		OpObject opObject = blc.getObjectByName("opr.place", "76H3X2", "uqbg6o");
		assertEquals("2021-09-14T00:56:24.909+0000", opObject.getFieldByExpr("source.osm[0].deleted"));
	}

	@Test
	public void testCheckImmutableObj() throws FailedVerificationException {
		blc.addOperation(createMergeOperation());
		OpObject obj = blocksManager.getBlockchain().getObjectByName("opr.place", "76H3X2", "uqbg6o");
		Map<String, List<Map<String, Object>>> sourcesObj = obj.getField(null, "source");

		List<Map<String, Object>> listValues = sourcesObj.get("osm");
		Collections.swap(listValues, 0, 1);

		//check the object in blockchain wasn't change
		OpObject oldObj = blocksManager.getBlockchain().getObjectByName("opr.place", "76H3X2", "uqbg6o");
		assertEquals("2021-09-14T00:56:24.909+0000", oldObj.getFieldByExpr("source.osm[0].deleted"));

		Map<String, List<Map<String, Object>>> newOsmMap = new HashMap<>();
		newOsmMap.put("osm", listValues);

		Exception exception = null;
		try {
			obj.setFieldByExpr("source", newOsmMap);
		} catch (UnsupportedOperationException t) {
			exception = t;
		}
		assertNotNull(exception);
	}

	private OpOperation createMergeOperation() throws FailedVerificationException {
		addOperationFromList(jsonFormatter, blc, new String[]{"create-obj-append-osm"});
		addOperationFromList(jsonFormatter, blc, new String[]{"create-obj-append-osm2"});
		OpObject oldObj = blocksManager.getBlockchain().getObjectByName("opr.place", "76H3X2", "uqbg6o");
		OpObject newObj = blocksManager.getBlockchain().getObjectByName("opr.place", "76H3X2", "uqbg62");

		//create merge op
		OpOperation editOp = new OpOperation();
		editOp.setType("opr.place");
		editOp.setSignedBy(serverName);

		OpObject editObj = new OpObject();
		editObj.setId(oldObj.getId().get(0), oldObj.getId().get(1));

		TreeMap<String, Object> current = new TreeMap<>();
		TreeMap<String, Object> changed = new TreeMap<>();

		Map<String, Object> newFields = newObj.getField(null, "source");
		Map<String, Object> oldFields = oldObj.getField(null, "source");

		for (Map.Entry<String, Object> newf : newFields.entrySet()) {
			TreeMap<String, Object> appendObj = new TreeMap<>();
			String category = "source" + "." + newf.getKey();
			List<Map<String, Object>> newCategoryList = (List<Map<String, Object>>) newf.getValue();
			if (!newCategoryList.isEmpty()) {
				if (oldFields == null || !oldFields.containsKey(newf.getKey())) {
					appendObj.put("set", newCategoryList);
				} else {
					if (newCategoryList.size() > 1) {
						appendObj.put("appendmany", newCategoryList);
					} else {
						appendObj.put("append", newCategoryList.get(0));
					}
					current.put(category, oldFields.get(newf.getKey()));
				}
				changed.put(category, appendObj);
			}
		}

		editObj.putObjectValue(OpObject.F_CHANGE, changed);
		editObj.putObjectValue(OpObject.F_CURRENT, current);

		editOp.addEdited(editObj);
		blc.getRules().generateHashAndSign(editOp, serverKeyPair);
		editOp.makeImmutable();
		return editOp;
	}
}
