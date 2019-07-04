package org.openplacereviews.opendb.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JsonMapUtilsTest {
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	private Gson gson;

	private String json;

	@Before
	public void beforeEachTestMethod() throws FailedVerificationException {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(TreeMap.class, new JsonFormatter.MapDeserializerDoubleAsIntFix());
		gson = builder.create();
		json = "{\"type\":\"place\"," +
				"\"append\":\"val2\"," +
				"\"tags\":{\"append\":{\"v\":\"2222222\",\"k\":\"333333333\"}}," +
				"\"xF\":{\"tags\":{\"append\":{\"v\":\"2222222\",\"k\":\"333333333\"}}}," +
				"\"update\": {\"arr\": [[1, 2, 3], [4, 5, 6], {\"field\": \"val\"}]}}";
	}

	@Test
	public void getFieldByFieldSequenceTest() {
		TreeMap jsonMap = new Gson().fromJson(json, TreeMap.class);
		Object fieldVal = JsonObjectUtils.getField(jsonMap, Arrays.asList("type"));
		String expectedFieldVal = "place";
		assertEquals(expectedFieldVal, fieldVal);

		Object fieldVal1 = JsonObjectUtils.getField(jsonMap, Arrays.asList("tags", "append"));
		Map arrEl = new TreeMap<>();
		arrEl.put("v", "2222222");
		arrEl.put("k", "333333333");
		assertEquals(arrEl, fieldVal1);

		Object fieldVal2 = JsonObjectUtils.getField(jsonMap, Arrays.asList("type"));
		String expectedFieldVal2 = "place";
		assertEquals(expectedFieldVal2, fieldVal2);

		Object fieldVal3 = JsonObjectUtils.getField(jsonMap, Arrays.asList("xF", "tags", "append", "v"));
		String expectedFieldVal3 = "2222222";
		assertEquals(expectedFieldVal3, fieldVal3);

		Object fieldVal4 = JsonObjectUtils.getField(jsonMap, Arrays.asList("update", "arr[2]", "field"));
		String expectedFieldVal4 = "val";
		assertEquals(expectedFieldVal4, fieldVal4);

		Object fieldVal5 = JsonObjectUtils.getField(jsonMap, Arrays.asList("update", "notExistField"));
		String expectedFieldVal5 = null;
		assertEquals(expectedFieldVal5, fieldVal5);
	}

	@Test(expected = ClassCastException.class)
	public void getNotExistFieldByFieldSequenceTest() {
		TreeMap jsonMap = new Gson().fromJson(json, TreeMap.class);
		Object res = JsonObjectUtils.getField(jsonMap, Arrays.asList("type", "notExist"));
	}

	@Test
	public void deleteFieldByFieldSequenceTest() {
		TreeMap jsonMap = new Gson().fromJson(json, TreeMap.class);
		JsonObjectUtils.deleteField(jsonMap, Arrays.asList("append"));
		JsonObjectUtils.deleteField(jsonMap, Arrays.asList("xF", "tags", "append"));

		String expectedJson = "{" +
				"\"type\" : \"place\"," +
				"\"tags\": {\"append\" : {\"v\": \"2222222\", \"k\": \"333333333\"} }," +
				"\"xF\": {\"tags\": {}}," +
				"\"update\": {\"arr\": [[1, 2, 3], [4, 5, 6], {\"field\": \"val\"}]}}";
		TreeMap expectedJsonMap = new Gson().fromJson(expectedJson, TreeMap.class);
		assertEquals(expectedJsonMap, jsonMap);
	}

	@Test
	public void setFieldByFieldSequenceTest() {
		TreeMap jsonMap = new Gson().fromJson(json, TreeMap.class);
		JsonObjectUtils.setField(jsonMap, Arrays.asList("type"), "someNewType");

		List<Double> numbers = Arrays.asList(2.2, 3.3, 4.4);
		JsonObjectUtils.setField(jsonMap, Arrays.asList("xF", "tags", "append"), numbers);

		String expectedJson = "{" +
				"\"type\" : \"someNewType\"," +
				"\"append\": \"val2\"," +
				"\"tags\": {\"append\" : {\"v\": \"2222222\", \"k\": \"333333333\"} }," +
				"\"xF\": {\"tags\": {\"append\" : [2.2, 3.3, 4.4]}}," +
				"\"update\": {\"arr\": [[1, 2, 3], [4, 5, 6], {\"field\": \"val\"}]}}";
		TreeMap expectedJsonMap = new Gson().fromJson(expectedJson, TreeMap.class);
		assertEquals(expectedJsonMap, jsonMap);

		JsonObjectUtils.setField(jsonMap, Arrays.asList("update", "arr[1]"), Arrays.asList(3.0, 3.0, 3.0));
		expectedJson = "{" +
				"\"type\" : \"someNewType\"," +
				"\"append\": \"val2\"," +
				"\"tags\": {\"append\" : {\"v\": \"2222222\", \"k\": \"333333333\"} }," +
				"\"xF\": {\"tags\": {\"append\" : [2.2, 3.3, 4.4]}}," +
				"\"update\": {\"arr\": [[1, 2, 3], [3, 3, 3], {\"field\": \"val\"}]}}";
		expectedJsonMap = new Gson().fromJson(expectedJson, TreeMap.class);
		assertEquals(expectedJsonMap, jsonMap);


		JsonObjectUtils.setField(jsonMap, Arrays.asList("update", "arr[2]", "field"), "newVal");
		expectedJson = "{" +
				"\"type\" : \"someNewType\"," +
				"\"append\": \"val2\"," +
				"\"tags\": {\"append\" : {\"v\": \"2222222\", \"k\": \"333333333\"} }," +
				"\"xF\": {\"tags\": {\"append\" : [2.2, 3.3, 4.4]}}," +
				"\"update\": {\"arr\": [[1, 2, 3], [3, 3, 3], {\"field\": \"newVal\"}]}}";
		expectedJsonMap = new Gson().fromJson(expectedJson, TreeMap.class);
		assertEquals(expectedJsonMap, jsonMap);
	}

	@Test
	public void setFieldByFieldSequenceIncorrectTest() {
		TreeMap jsonMap = new Gson().fromJson(json, TreeMap.class);
		JsonObjectUtils.setField(jsonMap, Arrays.asList("update", "arr[3]"), "someNewVal");

		IllegalArgumentException ex = null;
		try {
			JsonObjectUtils.setField(jsonMap, Arrays.asList("update", "array[3]"), "someNewVal");
		} catch (IllegalArgumentException e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	@Test
	public void getFieldByFieldSequenceIncorrectTest() {
		TreeMap jsonMap = new Gson().fromJson(json, TreeMap.class);
		Object res = JsonObjectUtils.getField(jsonMap, Arrays.asList("update", "arr[3]"));
		assertEquals(res, null);
		res = JsonObjectUtils.getField(jsonMap, Arrays.asList("update", "array[3]"));
		assertEquals(res, null);
		res = JsonObjectUtils.getField(jsonMap, Arrays.asList("Not", "exist", "sequence"));
		assertEquals(res, null);
	}



}
