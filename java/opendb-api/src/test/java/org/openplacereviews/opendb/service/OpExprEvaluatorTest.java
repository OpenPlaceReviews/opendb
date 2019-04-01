package org.openplacereviews.opendb.service;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.openplacereviews.opendb.util.OpExprEvaluator;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class OpExprEvaluatorTest {

	public static final String SIMPLE_JSON = "{'a':1, 'b': 'b', 'c' : ['1', '2'], 'e' : {'a': {'a':3}} }";

	public Object evaluateExpr(String e) {
		Gson gson = new Gson();
		JsonElement obj = gson.fromJson(SIMPLE_JSON, JsonElement.class);
		OpExprEvaluator.EvaluationContext ectx = new OpExprEvaluator.EvaluationContext(null, obj.getAsJsonObject(), null, null);
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
}
