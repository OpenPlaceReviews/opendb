package org.openplacereviews.opendb.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.junit.Test;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;

import java.security.KeyPair;

import static org.junit.Assert.assertEquals;
import static org.openplacereviews.opendb.ObjectGeneratorTest.generateOperations;
import static org.openplacereviews.opendb.VariableHelperTest.*;

public class OpExprEvaluatorTest {

	public static final String SIMPLE_JSON = "{'a':1, 'b': 'b', 'c' : ['1', '2'], 'e' : {'a': {'a':3}}, 'l' : ['1:3'], 'f':1," +
			"'array':['sys.grant', 'sys.login', 'sys.role', 'sys.signup', 'sys.validate', 'sys.grant', 'sys.operation']}";

	public Object evaluateExpr(String e) {
		Gson gson = new Gson();
		JsonElement obj = gson.fromJson(SIMPLE_JSON, JsonElement.class);
		OpExprEvaluator.EvaluationContext ectx = new OpExprEvaluator.EvaluationContext(null, obj.getAsJsonObject(),obj.getAsJsonObject().get("new"), null, null);
		return OpExprEvaluator.parseExpression(e).evaluateObject(ectx);
	}
	
	public void evaluateExprForException(String e) {
		try {
			evaluateExpr(e);
			org.junit.Assert.fail("Exception is expected for '" + e + "'");
		} catch (Exception e1) {
		}
	}

	@Test
	public void testSimpleIntegers() {
		assertEquals(1l, evaluateExpr("1"));
		assertEquals(1123l, evaluateExpr("1123"));
		assertEquals(-1123l, evaluateExpr("-1123"));
	}

	@Test
	public void testSimpleStrings() {
		assertEquals("1", evaluateExpr("'1'"));
		assertEquals("1\"'", evaluateExpr("'1\"\\\''"));
		assertEquals("1\"'", evaluateExpr("\"1\\\"\'\""));
		evaluateExprForException("\"1\"\'\"");
	}

	@Test
	public void testSimpleGet() {
		assertEquals(1, ((Number)evaluateExpr(".a")).intValue());
		assertEquals(1, ((Number)evaluateExpr("this.a")).intValue());
		assertEquals("b", evaluateExpr(".b"));
		assertEquals("[\"1\",\"2\"]", evaluateExpr(".c").toString());
		assertEquals("[\"1:3\"]", evaluateExpr(".l").toString());
		assertEquals(null, evaluateExpr(".d"));
		assertEquals(3, ((Number)evaluateExpr(".e.a.a")).intValue());
		assertEquals(3, ((Number)evaluateExpr("this.e.a.a")).intValue());
	}
	
	@Test
	public void testSimpleFunctionEval() {
		assertEquals(4l, evaluateExpr("m:plus(1,3)"));
		assertEquals(-2l, evaluateExpr("m:plus(1,-3)"));
		assertEquals("1", evaluateExpr("str:first('1:3')"));
		assertEquals("3", evaluateExpr("str:second('1:3')"));
	}

	@Test
	public void testFunction_Str_first() {
		assertEquals("1", evaluateExpr("str:first('1:3')"));
		assertEquals("1", evaluateExpr("str:first(this.l)"));
	}

	@Test
	public void  testFunction_Str_second() {
		assertEquals("3", evaluateExpr("str:second('1:3')"));
		assertEquals("3", evaluateExpr("str:second(this.l)"));
	}

	@Test
	public void testFunction_Str_combine() {
		assertEquals("12", evaluateExpr("str:combine('12',':')"));
	}

	@Test
	public void testFunction_M_plus() {
		assertEquals(4l, evaluateExpr("m:plus(1,3)"));
		assertEquals(-2l, evaluateExpr("m:plus(1,-3)"));
		evaluateExprForException("m:plus(this.a, this.c.0)");
	}

	@Test
	public void testFunction_M_mult() {
		assertEquals(3l, evaluateExpr("m:mult(1,3)"));
		assertEquals(-3l, evaluateExpr("m:mult(1,-3)"));
		evaluateExprForException("m:mult(this.a, this.c.0)");
	}

	@Test
	public void testFunction_M_div() {
		assertEquals(3l, evaluateExpr("m:div(3,1)"));
		assertEquals(3l, evaluateExpr("m:div(-9,-3)"));
		assertEquals(Double.NaN, evaluateExpr("m:div(0,0)"));
		assertEquals(Double.POSITIVE_INFINITY, evaluateExpr("m:div(1,0)"));
		assertEquals(Double.NEGATIVE_INFINITY, evaluateExpr("m:div(-1,0)"));
		evaluateExprForException("m:div(this.a, this.c.0)");
	}

	@Test
	public void testFunction_M_minus() {
		assertEquals(2l, evaluateExpr("m:minus(3,1)"));
		assertEquals(-6l, evaluateExpr("m:minus(-9,-3)"));
		evaluateExprForException("m:minus(this.a, this.c.0)");
	}

	@Test
	public void testFunction_Std_eq() {
		assertEquals(0, evaluateExpr("std:eq(3,1)"));
		assertEquals(1, evaluateExpr("std:eq(3,3)"));
		assertEquals(1, evaluateExpr("std:eq(this.a, this.f)"));
		assertEquals(1, evaluateExpr("std:eq(std:size(this.c), 2)"));
	}

	@Test
	public void testFunction_Std_neq() {
		assertEquals(1, evaluateExpr("std:neq(3,1)"));
		assertEquals(0, evaluateExpr("std:neq(3,3)"));
		assertEquals(0, evaluateExpr("std:neq(this.a, this.f)"));
		assertEquals(0, evaluateExpr("std:neq(std:size(this.c), 2)"));
	}

	@Test
	public void testFunction_Std_leq() {
		assertEquals(1, evaluateExpr("std:leq(0,1)"));
		assertEquals(0, evaluateExpr("std:leq(6,3)"));
		assertEquals(1, evaluateExpr("std:leq(this.a, this.f)"));
	}

	@Test
	public void testFunction_Std_le() {
		assertEquals(true, evaluateExpr("std:le(0,1)"));
		assertEquals(false, evaluateExpr("std:le(1,1)"));
		assertEquals(false, evaluateExpr("std:le(6,3)"));
		evaluateExprForException("std:le(this.a, this.c.0)");
	}

	@Test
	public void testFunction_Std_size() {
		assertEquals(1, evaluateExpr("std:size(this.a)"));
		assertEquals(1, evaluateExpr("std:size(this.e)"));
		assertEquals(2, evaluateExpr("std:size(this.c)"));
	}

	@Test
	public void testFunction_Std_or() {
		assertEquals(1, evaluateExpr("std:or(std:eq('1','1'), std:eq('2','2'))"));
		assertEquals(1, evaluateExpr("std:or(std:eq('1','1'), std:eq('2','1'))"));
		assertEquals(1, evaluateExpr("std:or(std:eq('1','2'), std:eq('2','2'))"));
		assertEquals(0, evaluateExpr("std:or(std:eq('1','2'), std:eq('2','1'))"));
	}

	@Test
	public void testFunction_Std_and() {
		assertEquals(1, evaluateExpr("std:and(std:eq('1','1'), std:eq('2','2'))"));
		assertEquals(0, evaluateExpr("std:and(std:eq('1','1'), std:eq('2','1'))"));
		assertEquals(0, evaluateExpr("std:and(std:eq('1','2'), std:eq('2','2'))"));
		assertEquals(0, evaluateExpr("std:and(std:eq('1','2'), std:eq('2','1'))"));
	}

	@Test
	public void testFunction_Set_in() {
		assertEquals(1, evaluateExpr("set:in('1', this.c)"));
		assertEquals(1, evaluateExpr("set:in('sys.operation', set:all('sys.operation','sys.validate','sys.role','sys.grant', 'sys.login', 'sys.signup'))"));
		assertEquals(0, evaluateExpr("set:in(set:all('sys.operation', 'sys.role'), set:all('sys.operation','sys.validate','sys.role','sys.grant', 'sys.login', 'sys.signup'))"));

	}

	@Test
	public void testFunction_Set_all() {
		assertEquals(6, evaluateExpr("std:size(set:all('sys.operation','sys.validate','sys.role','sys.grant', 'sys.login', 'sys.signup'))"));
	}

	@Test
	public void testFunction_Set_minus() {
		JsonArray jsonArray = new JsonArray();
		jsonArray.add("\"sys.login\"");
		jsonArray.add("\"sys.operation\"");
		jsonArray.add("\"sys.role\"");
		jsonArray.add("\"sys.signup\"");
		jsonArray.add("\"sys.validate\"");

		JsonArray jsonArray1 = new JsonArray();
		jsonArray1.add("\"sys.grant\"");

		assertEquals(jsonArray, evaluateExpr("set:minus(this.array, '\"sys.grant\"')"));
		assertEquals(new JsonArray(), evaluateExpr("set:minus('\"sys.grant\"', this.array)"));
		assertEquals(new JsonArray(), evaluateExpr("set:minus('', '\"sys.grant\"')"));
		assertEquals(jsonArray1, evaluateExpr("set:minus('\"sys.grant\"', '')"));
	}

	@Test
	public void testFunction_Auth_has_sig_roles() throws FailedVerificationException {
		OpBlockChain blc = generateBlockchain();

		String SIMPLE_JSON = "{\"hash\":\"json:sha256:f5779d3ab83282ad1b617d1d0daaf21fb54b0582b68c66b38233082a001ba0ae\",\"new\":[{\"comment\":\"Some comment\",\"id\":\"openplacereviews\",\"role\":\"\",\"type\":\"sys.operation\"}],\"old\":[{\"comment\":\"Validate validation changes. Check previous role. It is strict and nobody could change it\",\"id\":[\"sysvalidate_check_previous_role_for_change\"],\"if\":[\"std:eq(std:size(.old), 1)\"],\"role\":\"none\",\"type\":\"sys.validate\",\"validate\":[\"auth:has_sig_roles(this, .old.0.role)\"]}],\"signature\":\"ECDSA:base64:MEQCIB7cWUiAx5f3+kVfpcqnRF2Sx3Jcvxy09h54A8elAPcLAiAh43s5n2oHAMbbWGSnEnWfbV+m6tq09ZVgf+LX44UOFg==\",\"signed_by\":\"openplacereviews:test_1\",\"type\":\"sys.validate\",\"ref\":{\"op\":{\"arity\":0,\"comment\":\"Operation that defines validation on blockchain operations\",\"fields\":{\"id\":\"Unique name for validation\",\"if\":\"Array of preconditions (combined with AND) for validation to be applicable\",\"role\":\"Role of users could change that validation\",\"type\":\"Array of operations to which validation is applicable (* is a global)\",\"validate\":\"Array of validation rules (all should be evaluated to true or number != 0\"},\"id\":[\"sys.validate\"],\"version\":0,\"type\":\"sys.operation\"}}}";

		Gson gson = new Gson();
		JsonElement obj = gson.fromJson(SIMPLE_JSON, JsonElement.class);

		OpExprEvaluator.EvaluationContext ectx = new OpExprEvaluator.EvaluationContext(blc, obj.getAsJsonObject(), obj.getAsJsonObject().get("new"), null, null);

		assertEquals(1, OpExprEvaluator.parseExpression("auth:has_sig_roles(this, 'master')").evaluateObject(ectx));
		assertEquals(1, OpExprEvaluator.parseExpression("auth:has_sig_roles(this, 'administrator')").evaluateObject(ectx));
		assertEquals(1, OpExprEvaluator.parseExpression("auth:has_sig_roles(this, set:all('administrator', 'master'))").evaluateObject(ectx));
		assertEquals(0, OpExprEvaluator.parseExpression("auth:has_sig_roles(this, 'owner')").evaluateObject(ectx));
		assertEquals(0, OpExprEvaluator.parseExpression("auth:has_sig_roles(this, set:all('administrator', 'owner'))").evaluateObject(ectx));
	}

	@Test
	public void testFunction_Blc_find() {

	}

	private OpBlockChain generateBlockchain() throws FailedVerificationException {
		OpBlockChain blc;
		JsonFormatter formatter;
		KeyPair serverKeyPair;

		formatter = new JsonFormatter();
		blc = new OpBlockChain(OpBlockChain.NULL, new OpBlockchainRules(formatter, null));
		serverKeyPair = SecUtils.getKeyPair(SecUtils.ALGO_EC, serverKey, serverPublicKey);

		generateOperations(formatter, blc);
		blc.createBlock(serverName, serverKeyPair);

		return blc;
	}
}
