package org.openplacereviews.opendb.service;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class SimpleExprEvaluatorTest {

	public static final String SIMPLE_JSON = "{'a':1, 'b': 'b', 'c' : ['1', '2'], 'e' : {'a': {'a':3}} }";

	public Object evaluateExpr(String e) {
		Gson gson = new Gson();
		JsonElement obj = gson.fromJson(SIMPLE_JSON, JsonElement.class);
		SimpleExprEvaluator.EvaluationContext ectx = new SimpleExprEvaluator.EvaluationContext(null,
				obj.getAsJsonObject());
		return SimpleExprEvaluator.parseMappingExpression(e).execute(null, ectx);
	}
	
	public Object evaluateExprJson(String e) {
		JsonElement o = (JsonElement) evaluateExpr(e);
		if(o == null) {
			return null;
		}
		if(o.isJsonPrimitive()) {
			if(o.getAsJsonPrimitive().isNumber()) {
				return o.getAsInt();
			}
			return o.getAsString();
		}
		return o.toString();
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
		assertEquals(1, evaluateExprJson(".a"));
		assertEquals(1, evaluateExprJson("this.a"));
		assertEquals("b", evaluateExprJson(".b"));
		assertEquals("[\"1\",\"2\"]", evaluateExprJson(".c"));
		assertEquals(null, evaluateExprJson(".d"));
		assertEquals(3, evaluateExprJson(".e.a.a"));
		assertEquals(3, evaluateExprJson("this.e.a.a"));
	}
	
	@Test
	public void testSimpleFunctionEval() {
		assertEquals(4l, evaluateExpr("m.plus(1,3)"));
		assertEquals(-2l, evaluateExpr("m.plus(1,-3)"));
		assertEquals("1", evaluateExpr("str.first('1:3')"));
		assertEquals("3", evaluateExpr("str.second('1:3')"));
	}
}
