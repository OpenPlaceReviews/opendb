package org.openplacereviews.opendb.service;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class SimpleExprEvaluatorTest {

	public static final String SIMPLE_JSON = "{'a':1, 'b': 'b', 'c' : ['1', '2'] }";
	public Object evaluateExpr(String e) {
		Gson gson = new Gson();
		JsonElement obj = gson.fromJson(SIMPLE_JSON, JsonElement.class);
		SimpleExprEvaluator.EvaluationContext ectx = new SimpleExprEvaluator.EvaluationContext(null, obj.getAsJsonObject());
		return SimpleExprEvaluator.parseMappingExpression(e).execute(null,  ectx);
	}
	
	public void evaluateExprForException(String e) {
		try {
			evaluateExpr(e);
			org.junit.Assert.fail("Exception is expected for '" + e+"'");
		} catch (Exception e1) {
		}
	}
	
	@Test
	public void testSimpleIntegers() {
		assertEquals(1, evaluateExpr("1"));
		assertEquals(1123, evaluateExpr("1123"));
		assertEquals(-1123, evaluateExpr("-1123"));
	}
	
	@Test
	public void testSimpleStrings() {
		assertEquals("1", evaluateExpr("'1'"));
		assertEquals("1\"'", evaluateExpr("'1\"\\\''"));
		assertEquals("1\"'", evaluateExpr("\"1\\\"\'\""));
		evaluateExprForException("\"1\"\'\"");
	}
}
