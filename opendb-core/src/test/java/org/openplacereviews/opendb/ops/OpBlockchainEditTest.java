package org.openplacereviews.opendb.ops;

import org.junit.Before;
import org.junit.Test;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

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
		for (OpOperation opOperation : generateStartOpForTest()) {
			blc.addOperation(opOperation);
		}
	}

	@Test
	public void testEditAppendOp() {

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
		tagsObject.put("v", 11111111);
		tagsObject.put("k", 22222222);
		createObjForNewOpObject.putObjectValue("tags", Collections.singletonList(tagsObject));

		newOpObject.addCreated(createObjForNewOpObject);
		blc.getRules().generateHashAndSign(newOpObject, serverKeyPair);

		return Arrays.asList(initOp, newOpObject);
	}
}
