package org.openplacereviews.opendb.ops;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertNotNull;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

@RunWith(JUnitParamsRunner.class)
public class BlockManagerTest {

	public OpBlockChain blc;

	public BlocksManager blocksManager;

	public JsonFormatter formatter;

	private Object[] parametersWithBlockchainAndBlock() throws FailedVerificationException {
		beforeEachTestMethod();

		OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
		assertNotNull(opBlock);

		return new Object[]{
				blc
		};
	}

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		formatter = new JsonFormatter();
		blocksManager = new BlocksManager();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		generateOperations(formatter, blc);
	}

	@Test
	@Parameters(method = "parametersWithBlockchainAndBlock")
	public void testAddFixOperation(OpBlockChain blcDB) throws FailedVerificationException {
		OpBlock opBlock = blcDB.getFullBlockByRawHash(blcDB.getBlockHeadersById(0).getRawHash());
		blc.replicateBlock(opBlock);
		blocksManager.addFixOperation(blc, opBlock, 0, "/fixOperations/place_id_76H3X2_uqbg6o.json");
		OpObject opObject = blc.getObjectByName("osm.place", "76H3X2", "uqbg6o");

		assertEquals("111168845", opObject.getFieldByExpr("source.osm[0].changeset"));
	}
}
