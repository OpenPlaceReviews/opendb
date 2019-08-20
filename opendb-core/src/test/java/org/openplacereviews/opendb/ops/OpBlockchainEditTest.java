package org.openplacereviews.opendb.ops;

import org.junit.Before;
import org.junit.Test;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateUserOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_OPERATION;

public class OpBlockchainEditTest {

	private static final String OBJ_ID = "123456678";
	private static final String OP_ID = "osm.testplace";

	public OpBlockChain blc;

	public JsonFormatter jsonFormatter;

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		jsonFormatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(jsonFormatter, null));
		generateUserOperations(jsonFormatter, blc);
		for (OpOperation opOperation : generateStartOpForTest()) {
			opOperation.makeImmutable();
			blc.addOperation(opOperation);
		}
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
	public void testEditIncrementOp() {
	}

	@Test
	public void testEditDeleteOp() {

	}

	@Test
	public void testEditSetOp() {

	}

	@Test
	public void testEditWithRefFieldsOnNewObject() {

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
		createObjForNewOpObject.putObjectValue("lon", 12345);
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
}
