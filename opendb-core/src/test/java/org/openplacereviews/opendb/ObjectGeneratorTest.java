package org.openplacereviews.opendb;

import org.openplacereviews.opendb.api.MgmtController;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.DBConstants;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.DBConsensusManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.InputStreamReader;
import java.security.KeyPair;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;

public class ObjectGeneratorTest {

	public static String[] BOOTSTRAP_LIST =
			new String[]{"opr-0-test-user", "history-test", "std-ops-defintions", "std-roles", "opr-0-test-user-test",
					"opr-0-test-grant", "std-validations"};

	public static String[] BLOCKCHAIN_LIST =
			new String[]{"opr-0-test-user", "std-roles", "opr-0-test-grant", "std-ops-defintions", "opr-0-test-user-test","std-validations", "voting-process"};

	public static String[] USER_LIST =
			new String[]{"opr-0-test-user"};

	public static String[] VOTING_LIST =
			new String[]{"opr-0-test-user", "std-roles", "opr-0-test-grant", "voting-validation"};

	public static void generateUserOperations(JsonFormatter formatter, OpBlockChain blc) throws
			FailedVerificationException {
		addOperationFromList(formatter, blc, USER_LIST);
	}

	public static void generateVotingOperations(JsonFormatter formatter, OpBlockChain blc) throws
			FailedVerificationException {
		addOperationFromList(formatter, blc, VOTING_LIST);
	}

	public static List<OpOperation> getVotingOperations(JsonFormatter formatter,  OpBlockChain blc, int amount) throws FailedVerificationException {
		OpOperation[] lst = formatter.fromJson(
				new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/voting-process.json")),
				OpOperation[].class);
		for (int i = 0 ; i < amount; i++) {
			if (!OUtils.isEmpty(serverName) && lst[i].getSignedBy().isEmpty()) {
				lst[i].setSignedBy(serverName);
				lst[i] = blc.getRules().generateHashAndSign(lst[i], serverKeyPair);
			}
		}
		return Arrays.asList(lst).subList(0, amount);
	}

	private static void addOperationFromList(JsonFormatter formatter, OpBlockChain blc, String[] userList) throws FailedVerificationException {
		for (String f : userList) {
			OpOperation[] lst = formatter.fromJson(
					new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/" + f + ".json")),
					OpOperation[].class);
			for (OpOperation o : lst) {
				if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
					o.setSignedBy(serverName);
					o = blc.getRules().generateHashAndSign(o, serverKeyPair);
				}
				o.makeImmutable();
				blc.addOperation(o);
			}
		}
	}

	public static List<OpOperation> getOperations(JsonFormatter formatter, BlocksManager blocksManager, String[] name_list) throws FailedVerificationException {
		List<OpOperation> allOperation = new ArrayList<>();
		for (String f : name_list) {
			OpOperation[] lst = formatter.fromJson(
					new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/" + f + ".json")),
					OpOperation[].class);
			for (OpOperation o : lst) {
				if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
					o.setSignedBy(serverName);
					o = blocksManager.generateHashAndSign(o, serverKeyPair);
				}
				o.makeImmutable();
				allOperation.add(o);
			}

		}

		return allOperation;

	}

	public static void generateOperations(JsonFormatter formatter, OpBlockChain blc) throws FailedVerificationException {
		addOperationFromList(formatter, blc, BOOTSTRAP_LIST);
	}

	public static void generateMore30Blocks(JsonFormatter formatter, OpBlockChain blc,
											DBConsensusManager dbConsensusManager, String[] name_list) throws FailedVerificationException {
		int i = 0;
		for (String f : name_list) {
			OpOperation[] lst = formatter.fromJson(
					new InputStreamReader(MgmtController.class.getResourceAsStream("/bootstrap/" + f + ".json")),
					OpOperation[].class);

			for (OpOperation o : lst) {
				if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
					o.setSignedBy(serverName);
					o = blc.getRules().generateHashAndSign(o, serverKeyPair);
				}
				o.makeImmutable();
				blc.addOperation(o);
				dbConsensusManager.insertOperation(o);
				if (i >= 3) {
					OpBlock opBlock = blc.createBlock(serverName, serverKeyPair);
					dbConsensusManager.insertBlock(opBlock);
				}
				i++;
			}
		}
	}

	public static void generateHashAndSignForOperation(OpOperation opOperation, OpBlockChain blc, boolean signedBy,
													   KeyPair... keyPair) throws FailedVerificationException {
		if (signedBy) {
			opOperation.setSignedBy(serverName);
		}

		blc.getRules().generateHashAndSign(opOperation, keyPair);
	}

	/**
	 * Allows to generate JSON with big size for creating Error Type.OP_SIZE_IS_EXCEEDED
	 *
	 * @return - String Json
	 */
	public static String generateBigJSON() {
		StringBuilder startOperation =
				new StringBuilder(
						"{\n" +
								"\t\t\"type\" : \"sys.grant\",\n" +
								"\t\t\"ref\" : {\n" +
								"\t\t\t\"s\" : [\"sys.signup\",\"openplacereviews\"]\n" +
								"\t\t},\n" +
								"\t\t\"new\" : ["
				);

		while (startOperation.length() <= OpBlockchainRules.MAX_OP_SIZE_MB) {
			for (int i = 0; i < 100; i++) {
				startOperation
						.append("\t\t{ \n" +
								"\t\t\t\"id\" : [\"openplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviews")
						.append(startOperation.length())
						.append(i)
						.append("\"],\n")
						.append("\t\t\t\"roles\" : [\"owner\"]\n")
						.append("\t\t},");
			}
		}

		startOperation
				.append("\t\t{ \n" +
						"\t\t\t\"id\" : [\"openplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviewsopenplacereviews\"],\n" +
						"\t\t\t\"roles\" : [\"owner\"]\n" + "\t\t}]\n" + "\t}");

		return startOperation.toString();
	}

	public static void generateMetadataDB(OpenDBServer.MetadataDb metadataDb, JdbcTemplate jdbcTemplate) {
		try {
			DatabaseMetaData mt = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection().getMetaData();
			ResultSet rs = mt.getColumns(null, DBConstants.SCHEMA_NAME, null, null);
			while (rs.next()) {
				String tName = rs.getString("TABLE_NAME");
				if (!metadataDb.tablesSpec.containsKey(tName)) {
					metadataDb.tablesSpec.put(tName, new ArrayList<>());
				}
				List<OpenDBServer.MetadataColumnSpec> cols = metadataDb.tablesSpec.get(tName);
				OpenDBServer.MetadataColumnSpec spec = new OpenDBServer.MetadataColumnSpec();
				spec.columnName = rs.getString("COLUMN_NAME");
				spec.sqlType = rs.getInt("DATA_TYPE");
				spec.dataType = rs.getString("TYPE_NAME");
				spec.columnSize = rs.getInt("COLUMN_SIZE");
				cols.add(spec);
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Can't read db metadata", e);
		}
	}
}
