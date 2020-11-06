package org.openplacereviews.opendb.ops;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.util.JsonFormatter;

import java.security.KeyPair;
import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateHashAndSignForOperation;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateOperations;
import static org.openplacereviews.opendb.VariableHelperTest.serverKeyPair;
import static org.openplacereviews.opendb.VariableHelperTest.serverName;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.*;
import static org.openplacereviews.opendb.ops.OpObject.F_COMMENT;

public class OpBlockchainRulesSysValidationTest {

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	private OpBlockChain blc;
	private JsonFormatter jsonFormatter;

	@Before
	public void beforeEachTestMethod() throws Exception {
		if (jsonFormatter == null) {
			jsonFormatter = new JsonFormatter();
		}
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(jsonFormatter, null));
		generateOperations(jsonFormatter, blc);
		blc.createBlock(serverName, serverKeyPair);
	}

	//////////////////////////////////////////////////////// SYS.SIGNUP ///////////////////////////////////////////////////////

	/**
	 * sys.signup operation
	 * Success self signing with pwd method
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSelfSignedForSysSignupWithPwdMethod() throws FailedVerificationException {
		generateSysSignupWithPwdAuth("elen");
	}

	/**
	 * sys.signup operation
	 * Success self signing with oauth method
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSelfSignedForSysSignupWithOauthMethod() throws FailedVerificationException {
		String salt = "someName";
		String oauthProvider = "googleId";

		OpObject opObject = new OpObject();
		opObject.setId(salt);
		opObject.putStringValue(OpObject.F_NAME, salt);
		opObject.putStringValue(OpBlockchainRules.F_OAUTHID_HASH,
				SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, salt, oauthProvider));
		opObject.putStringValue(OpBlockchainRules.F_OAUTH_PROVIDER, oauthProvider);

		OpOperation opOperation = new OpOperation();
		opOperation.setSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * sys.signup operation
	 * Success self signing with provided method
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSelfSignedForSysSignupWithProvidedMethod() throws FailedVerificationException {
		generateSysSignupWithProvidedAuth();
	}

	/**
	 * sys.signup operation
	 * Expected validation error: signup_self_signed_pwd
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSelfSignedForSysSignupExpectErrorValidation_SignupSelfSignedPwd() throws
			FailedVerificationException {
		String name = "elen1";
		String password = "149814981498a";

		KeyPair keyPair =
				SecUtils.generateKeyPairFromPassword(SecUtils.ALGO_EC, SecUtils.KEYGEN_PWD_METHOD_1, name, password, true);

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PWD);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		OpOperation opOperation = new OpOperation();
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.signup operation
	 * Expected validation error: signup_self_signed_provided
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSelfSignedForSysSignupExpectErrorValidation_SignupSelfSignedProvided() throws
			FailedVerificationException {
		String name = "openplacereviews1";
		KeyPair keyPair = SecUtils.generateRandomEC256K1KeyPair();

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(OpObject.F_NAME, name);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PROVIDED);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		OpOperation opOperation = new OpOperation();
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.signup operation
	 * Expected validation error: signup_login_only_by_administrator
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSelfSignedForSysSignupExpectErrorValidation_SignupLoginOnlyByAdministrator() throws
			FailedVerificationException {
		String name = "openplacereviews";
		KeyPair keyPair = SecUtils.generateRandomEC256K1KeyPair();

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(OpObject.F_NAME, name);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PROVIDED);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		OpOperation opOperation = new OpOperation();
		opOperation.addOtherSignedBy(name + 1);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.getRules().validateRules(blc, opOperation, new OpBlockChain.LocalValidationCtx(null));
	}


	//////////////////////////////////////////////////////// SYS.LOGIN ///////////////////////////////////////////////////////

	/**
	 * sys.login operation
	 * Success creating sys.login operation by pwd
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysLoginSuccessWithPwd() throws FailedVerificationException {
		String name = "elen", name1 = "test_1", password = "149814981498a";
		generateSysSignupWithPwdAuth(name);
		KeyPair keyPair = SecUtils.generateRandomEC256K1KeyPair();

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.addOrSetStringValue(OpObject.F_ID, name1);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setSignedBy(name);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addCreated(opObject);

		keyPair = SecUtils.generateEC256K1KeyPairFromPassword(name, password, false);
		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * sys.login operation
	 * expected validation error: signup_login_only_by_administrator
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysLoginExpectError_SignupLoginOnlyByAdministrator() throws FailedVerificationException {
		String name = "elen", name1 = "test_1", password = "149814981498a";
		KeyPair keyPair = SecUtils.generateRandomEC256K1KeyPair();
		generateSysSignupWithPwdAuth(name);

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.addOrSetStringValue(OpObject.F_ID, name1);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setSignedBy(name);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addCreated(opObject);

		keyPair = SecUtils.generateEC256K1KeyPairFromPassword(name, password, false);
		generateHashAndSignForOperation(opOperation, blc, false, keyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.login operation
	 * expected validation error: login_should_reference_its_signup
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysLoginExpectError_LoginShouldReferenceItsSignup() throws FailedVerificationException {
		String name = "openplace";
		KeyPair keyPair = generateSysSignupWithProvidedAuth();

		OpObject opObject = new OpObject();
		opObject.setId(name + 1);
		opObject.addOrSetStringValue(OpObject.F_ID, name + 1);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setSignedBy(name);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.login operation
	 * Expected validation error: login_self_signed_provided
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysLoginExpectError_LoginSelfSignedProvided() throws FailedVerificationException {
		String refName = "openplacereviews", name = "open", name1 = "test_1";
		KeyPair keyPair = generateSysSignupWithProvidedAuth();

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.addOrSetStringValue(OpObject.F_ID, name1);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, refName));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.login operation
	 * Expected validation error: login_update_self_signed_provided
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysLoginExpectError_LoginUpdateSelfSignedProvided() throws FailedVerificationException {
		String name = "openplacereviews", sName = "openplace", name1 = "test_1";
		KeyPair keyPair = generateSysSignupWithProvidedAuth();

		OpObject opObject = new OpObject();
		opObject.setId(sName);
		opObject.addOrSetStringValue(OpObject.F_ID, name1);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setSignedBy(sName);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addDeleted(Collections.singletonList(blc.getRules().getLoginKeyObj(blc, name).getParentHash()));
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.login operation
	 * Expected validation error: login_self_signed_pwd
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysLoginExpectError_LoginSelfSignedPwd() throws FailedVerificationException {
		String name = "elen", name1 = "test_1", pwd = "openplacereviewsPWD";
		generateSysSignupWithPwdAuth("elen");

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.addOrSetStringValue(OpObject.F_ID, name1);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, pwd));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.login operation
	 * Expected validation error: login_update_self_signed_pwd
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysLoginExpectError_LoginUpdateSelfSignedPwd() throws FailedVerificationException {
		String name = "elen", name1 = "elen1", name2 = "test_1", pwd = "openplacereviewsPWD";
		generateSysSignupWithPwdAuth(name);
		KeyPair keyPair = generateSysSignupWithPwdAuth(name1);

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.addOrSetStringValue(OpObject.F_ID, name2);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, pwd));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setSignedBy(name1);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opOperation.addDeleted(Collections.singletonList(blc.getRules().getLoginKeyObj(blc, name).getParentHash()));
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	//////////////////////////////////////////////////////// SYS.ROLE ///////////////////////////////////////////////////////

	/**
	 * sys.role operation
	 * Success validation for creating new role
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCreateRole() throws FailedVerificationException {
		String role = "user", ownerRole = "administrator";

		OpOperation opOperation = getOpRoleOperation(role, ownerRole, "", serverName);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * sys.role operation
	 * Expected validation error: role_none_could_not_be_created
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCreateRoleExpectError_RoleNoneCouldNotBeCreated() throws FailedVerificationException {
		String role = "none", ownerRole = "administrator";

		OpOperation opOperation = getOpRoleOperation(role, ownerRole, "", serverName);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * sys.role operation
	 * Success validation for changing role
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testChangeRole() throws FailedVerificationException {
		String role = "user", ownerRole = "administrator", newOwnerRole = "master", comment = "some comment",
				newComment = "some comment 1";

		// generate role with owner_role = administrator
		OpOperation opOperation = getOpRoleOperation(role, ownerRole, comment, serverName);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);

		// change comment and owner_role for user role
		opOperation = getOpRoleOperation(role, newOwnerRole, newComment, serverName);
		opOperation.addOtherSignedBy(serverName);

		// create delete op
		OpOperation deleteOperation = new OpOperation();
		deleteOperation.setType(OpBlockchainRules.OP_ROLE);
		deleteOperation.addDeleted(Collections.singletonList(role));
		generateHashAndSignForOperation(deleteOperation, blc, true, serverKeyPair);
		deleteOperation.makeImmutable();
		blc.addOperation(deleteOperation);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();
		blc.addOperation(opOperation);
	}

	/**
	 * sys.role operation
	 * Expected validation error: role_could_be_changed_only_by_owner
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testChangeRoleExpectError_RoleCouldBeChangedOnlyByOwner() throws FailedVerificationException {
		String role = "user", ownerRole = "administrator", newOwnerRole = "master", notOwner = "admin", comment =
				"some comment", newComment = "some comment 1";
		String operationHash;

		// generate role with owner_role = administrator
		OpOperation opOperation = getOpRoleOperation(role, ownerRole, comment, serverName);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
		operationHash = opOperation.getRawHash();
		KeyPair keyPair = generateSysSignupWithPwdAuth(notOwner);

		// change comment and onwer_role for user role
		opOperation = getOpRoleOperation(role, newOwnerRole, newComment, notOwner);
		opOperation.addDeleted(Collections.singletonList(operationHash));

		generateHashAndSignForOperation(opOperation, blc, false, keyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	//////////////////////////////////////////////////////// SYS.GRANT ///////////////////////////////////////////////////////

	/**
	 * sys.grant operation
	 * expected success granting role for user
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testGrantRole() throws FailedVerificationException {
		// generate user role
		String username = "testUsername", role = "user", ownerRole = "master", newOwnerRole = "administrator", comment =
				"some comment", newComment = "some comment 1";

		KeyPair userKeyPair = generateSysSignupWithPwdAuth(username);

		OpOperation opOperation = getOpRoleOperation(role, ownerRole, comment, serverName);

		generateHashAndSignForOperation(opOperation, blc, false, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);

		opOperation = getOpDeletedRoleOperation(role, username);

		generateHashAndSignForOperation(opOperation, blc, false, userKeyPair);
		opOperation.makeImmutable();

		Throwable e = null;
		try {
			blc.addOperation(opOperation);
		} catch (Throwable ex) {
			e = ex;
		}
		assertTrue(e instanceof BlockchainValidationException);

		// Grant role for user by owner role
		OpObject opObject = new OpObject();
		opObject.setId(username);
		opObject.putStringValue(F_ROLES, ownerRole);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, username));

		OpOperation grantOperation = new OpOperation();
		grantOperation.putObjectValue(OpOperation.F_REF, refs);
		grantOperation.setType(OpBlockchainRules.OP_GRANT);
		grantOperation.addCreated(opObject);

		generateHashAndSignForOperation(grantOperation, blc, true, serverKeyPair);
		grantOperation.makeImmutable();

		blc.addOperation(grantOperation);

		// checking to change role by not role owner
		opOperation = getOpRoleOperation(role, newOwnerRole, newComment, username);

		generateHashAndSignForOperation(opOperation, blc, false, userKeyPair);
		opOperation.makeImmutable();
		// checking to change role by granted owner
		blc.addOperation(opOperation);

		opOperation = getOpDeletedRoleOperation(role, username);

		generateHashAndSignForOperation(opOperation, blc, false, userKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * sys.role operation
	 * Expected validation error: grant_check_op_role_and_check_assigned_role
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testGrantRoleExpectError_GrantCheckOpRoleAndCheckAssignedRole() throws FailedVerificationException {
		// 1. generate user
		String username = "testUsername", ownerRole = "master";
		KeyPair userKeyPair = generateSysSignupWithPwdAuth(username);

		// 2. Grant role master for User by this User keyPair -> expect error
		OpObject opObject = new OpObject();
		opObject.setId(username);
		opObject.putStringValue(F_ROLES, ownerRole);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, username));

		OpOperation grantOperation = new OpOperation();
		grantOperation.setSignedBy(username);
		grantOperation.putObjectValue(OpOperation.F_REF, refs);
		grantOperation.setType(OpBlockchainRules.OP_GRANT);
		grantOperation.addCreated(opObject);

		generateHashAndSignForOperation(grantOperation, blc, false, userKeyPair);
		grantOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(grantOperation);
	}

	//////////////////////////////////////////////////////// SYS.VALIDATE ///////////////////////////////////////////////////////

	/**
	 * Expected validation error: sysvalidate_check_previous_role_for_change
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysValidateExpectError_SysValidateCheckPreviousRoleForChange() throws FailedVerificationException {
		OpObject loadedObject =
				blc.getObjectByName(OpBlockchainRules.OP_VALIDATE, "sys_validate_check_previous_role_for_change");
		assertNotNull(loadedObject);

		String newId = "openplacereviews", comment = "Some comment";

		OpObject opObject = new OpObject();
		opObject.setId(newId);
		opObject.putStringValue(F_TYPE, OP_OPERATION);
		opObject.putStringValue(F_COMMENT, comment);
		opObject.putObjectValue("role", "");

		OpOperation grantOperation = new OpOperation();
		grantOperation.setType(OpBlockchainRules.OP_VALIDATE);
		grantOperation.addDeleted(Collections.singletonList(loadedObject.getParentHash()));
		grantOperation.addCreated(opObject);

		generateHashAndSignForOperation(grantOperation, blc, true, serverKeyPair);
		grantOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(grantOperation);
	}

	/**
	 * Expected validation error: sysvalidate_check_previous_role_for_change
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testSysValidateSuccessCheckPreviousRoleForChange() throws FailedVerificationException {
		String name = "openplacereviewsPWD", validationName = "sys_validate_check_previous_role_for_change", comment =
				"Some comment";

		OpObject loadedObject = blc.getObjectByName(OpBlockchainRules.OP_VALIDATE, validationName);
		assertNotNull(loadedObject);

		OpObject opObject = new OpObject();
		opObject.setId(validationName);
		opObject.putStringValue(F_TYPE, OP_OPERATION);
		opObject.putStringValue(F_COMMENT, comment);
		opObject.putStringValue("role", "master");

		OpOperation grantOperation = new OpOperation();
		grantOperation.setSignedBy(name);
		grantOperation.setType(OpBlockchainRules.OP_VALIDATE);
		grantOperation.addCreated(opObject);

		generateHashAndSignForOperation(grantOperation, blc, false, serverKeyPair);
		grantOperation.makeImmutable();
		blc.addOperation(grantOperation);

		OpOperation delGrantOperation = new OpOperation();
		delGrantOperation.setSignedBy(name);
		delGrantOperation.setType(OpBlockchainRules.OP_VALIDATE);
		delGrantOperation.addDeleted(Collections.singletonList(validationName));

		generateHashAndSignForOperation(delGrantOperation, blc, false, serverKeyPair);
		delGrantOperation.makeImmutable();
		blc.addOperation(delGrantOperation);
	}

	//////////////////////////////////////////////////////// SYS.OPERATION ///////////////////////////////////////////////////////

	/**
	 * Success checking for existing type: sys.grant
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheckOperationType_OpGrant() throws FailedVerificationException {
		String name = "openplacereviews", role = "master";

		OpObject opObject = new OpObject();
		opObject.setId(name + 1);
		opObject.putStringValue(F_ROLES, role);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation grantOperation = new OpOperation();
		grantOperation.putObjectValue(OpOperation.F_REF, refs);
		grantOperation.setType(OpBlockchainRules.OP_GRANT);
		grantOperation.addCreated(opObject);

		generateHashAndSignForOperation(grantOperation, blc, true, serverKeyPair);
		grantOperation.makeImmutable();

		blc.addOperation(grantOperation);
	}

	/**
	 * Success checking for existing type: sys.login
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheckOperationType_OpLogin() throws FailedVerificationException {
		String name = "openplace";
		KeyPair keyPair = generateSysSignupWithPwdAuth(name);

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);

		Map<String, Object> refs = new TreeMap<>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, name));

		OpOperation opOperation = new OpOperation();
		opOperation.putObjectValue(OpOperation.F_REF, refs);
		opOperation.setSignedBy(name);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_LOGIN);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * Success checking for existing type: sys.role
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheckOperationType_OpRole() throws FailedVerificationException {
		String name = "user", comment = "some comment", ownerRole = "administrator";

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(OpObject.F_COMMENT, comment);
		opObject.putStringValue("owner_role", ownerRole);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_ROLE);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * Success checking for existing type: sys.operation
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheckOperationType_OpOperation() throws FailedVerificationException {
		String newOperation = "sys.operation1", comment = "some comment";

		OpObject opObject = new OpObject();
		opObject.setId(newOperation);
		opObject.putStringValue(F_COMMENT, comment);
		opObject.putObjectValue("arity", 0);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_OPERATION);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * Success checking for existing type: sys.validate
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheckOperationType_OpValidate() throws FailedVerificationException {
		String name = "all_op_arity_new_del1", comment = "Validate operation arity", role = "none";
		String ifStatement = "std:eq(.ref.op.arity, 0)", validateStatement1 = "std:leq(std:size(.new),1)",
				validateStatement2 = "std:leq(std:size(.old),1)";

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(F_TYPE, "*");
		opObject.putStringValue(F_COMMENT, comment);
		opObject.putStringValue(F_ROLES, role);
		opObject.putStringValue(F_IF, ifStatement);
		opObject.putStringValue(F_VALIDATE, validateStatement1);
		opObject.addOrSetStringValue(F_VALIDATE, validateStatement2);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(OpBlockchainRules.OP_VALIDATE);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}

	/**
	 * Success checking for existing type: sys.signup
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheckOperationType_OpSignup() throws FailedVerificationException {
		String name = "test";
		String password = "149814981498a";

		KeyPair keyPair = SecUtils.generateEC256K1KeyPairFromPassword(name, password, true);

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(OpObject.F_NAME, name);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		OpOperation opOperation = new OpOperation();
		opOperation.setSignedBy(name);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
	}


	//////////////////////////////////////////////////////// SYS.* ///////////////////////////////////////////////////////

	/**
	 * Expected validation error: all_op_type_registered for not existing type
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheckNotExistingType() throws FailedVerificationException {
		String newRole = "user", notExistingOperationType = "sys.role1", ownerRole = "administrator";

		OpObject opObject = new OpObject();
		opObject.setId(newRole);
		opObject.addOrSetStringValue(OpObject.F_COMMENT, "");
		opObject.putStringValue("owner_role", ownerRole);

		OpOperation opOperation = new OpOperation();
		opOperation.setType(notExistingOperationType);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, true, serverKeyPair);
		opOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(opOperation);
	}

	/**
	 * Expected validation error: all_op_arity_new_del
	 */
	@Test
	public void testCheck_AllOpArityNewDel() throws FailedVerificationException {
		String name = "openplacereviewsPWD", validationName = "sys_validate_check_previous_role_for_change", comment =
				"Some comment";

		OpObject loadedObject = blc.getObjectByName(OpBlockchainRules.OP_VALIDATE, validationName);
		assertNotNull(loadedObject);

		OpObject opObject = new OpObject();
		opObject.setId(validationName + 1);
		opObject.putStringValue(F_TYPE, OP_OPERATION);
		opObject.putStringValue(F_COMMENT, comment);
		opObject.putObjectValue("role", "master");

		OpOperation grantOperation = new OpOperation();
		grantOperation.setSignedBy(name);
		grantOperation.setType(OpBlockchainRules.OP_VALIDATE);
		grantOperation.addDeleted(Collections.singletonList(loadedObject.getParentHash()));
		grantOperation.addDeleted(Collections.singletonList(loadedObject.getParentHash()));
		grantOperation.addCreated(opObject);

		generateHashAndSignForOperation(grantOperation, blc, false, serverKeyPair);
		grantOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(grantOperation);
	}

	/**
	 * Expected validation error: all_op_arity_same_type_and_id
	 *
	 * @throws FailedVerificationException
	 */
	@Test
	public void testCheck_AllOpAritySameTypeAndId() throws FailedVerificationException {
		String name = "openplacereviewsPWD", validationName = "sys_validate_check_previous_role_for_change", comment =
				"Some comment";

		OpObject loadedObject = blc.getObjectByName(OpBlockchainRules.OP_VALIDATE, validationName);
		assertNotNull(loadedObject);

		OpObject opObject = new OpObject();
		opObject.setId(validationName + 1);
		opObject.putStringValue(F_TYPE, OP_OPERATION);
		opObject.putStringValue(F_COMMENT, comment);
		opObject.putObjectValue("role", "master");

		OpOperation grantOperation = new OpOperation();
		grantOperation.setSignedBy(name);
		grantOperation.setType(OpBlockchainRules.OP_VALIDATE);
		grantOperation.addDeleted(Collections.singletonList(loadedObject.getParentHash()));
		grantOperation.addCreated(opObject);

		generateHashAndSignForOperation(grantOperation, blc, false, serverKeyPair);
		grantOperation.makeImmutable();

		exceptionRule.expect(BlockchainValidationException.class);
		blc.addOperation(grantOperation);
	}

	//////////////////////////////////////////////////////// TEST HELPERS ///////////////////////////////////////////////////////

	private KeyPair generateSysSignupWithPwdAuth(String name) throws FailedVerificationException {
		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
		String password = "149814981498a";

		KeyPair keyPair = SecUtils.generateKeyPairFromPassword(SecUtils.ALGO_EC, keyGen, name, password, true);

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(F_TYPE, OP_SIGNUP);
		opObject.putStringValue(OpObject.F_NAME, name);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		OpOperation opOperation = new OpOperation();
		opOperation.setSignedBy(name);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
		blc.createBlock(serverName, serverKeyPair);

		return keyPair;
	}

	private KeyPair generateSysSignupWithProvidedAuth() throws FailedVerificationException {
		String name = "openplace";
		String password = "149814981498a";

		KeyPair keyPair = SecUtils.generateEC256K1KeyPairFromPassword(name, password, true);

		OpObject opObject = new OpObject();
		opObject.setId(name);
		opObject.putStringValue(OpObject.F_NAME, name);
		opObject.putStringValue(F_ALGO, SecUtils.ALGO_EC);
		opObject.putStringValue(OpBlockchainRules.F_PUBKEY,
				SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));

		OpOperation opOperation = new OpOperation();
		opOperation.setSignedBy(name);
		opOperation.addOtherSignedBy(serverName);
		opOperation.setType(OpBlockchainRules.OP_SIGNUP);
		opOperation.addCreated(opObject);

		generateHashAndSignForOperation(opOperation, blc, false, keyPair, serverKeyPair);
		opOperation.makeImmutable();

		blc.addOperation(opOperation);
		return keyPair;
	}

	private OpOperation getOpRoleOperation(String role, String ownerRole, String comment, String signedBy) {
		OpObject opObject = new OpObject();
		opObject.setId(role);
		opObject.addOrSetStringValue(OpObject.F_COMMENT, comment);
		opObject.putStringValue("owner_role", ownerRole);
		opObject.putStringValue("super_roles", "");

		OpOperation opOperation = new OpOperation();
		opOperation.addOtherSignedBy(signedBy);
		opOperation.setType(OpBlockchainRules.OP_ROLE);
		opOperation.addCreated(opObject);
		return opOperation;
	}

	private OpOperation getOpDeletedRoleOperation(String role, String signedBy) {
		OpOperation opOperation = new OpOperation();
		opOperation.addOtherSignedBy(signedBy);
		opOperation.setType(OpBlockchainRules.OP_ROLE);
		opOperation.addDeleted(Arrays.asList(role));

		return opOperation;
	}


}
