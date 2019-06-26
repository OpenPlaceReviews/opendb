package org.openplacereviews.opendb.util;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class uses for work with Json Object represent as Map.
 */
public class JsonMapUtils {

	public static final String ARRAY_FIELD_REGEX = "^((?!\\[).)+(\\[\\d+\\])+";

	private static final Pattern ARRAY_INDEXES_PATTERN = Pattern.compile("\\[\\d+\\]");

	/**
	 * Check is field presented as filed sequence array exist.
	 * @param jsonMap       source json object deserialized in map
	 * @param fieldSequence Sequence to field value.
	 *                      Example: person.car.number have to be ["person", "car[1]", "number"]
	 * @return if field exist then true
	 */
	public static boolean isFieldExist(Map<String, Object> jsonMap, String[] fieldSequence) {
		Object fieldValue = null;
		try {
			fieldValue = getField(jsonMap, fieldSequence);
		} catch (Exception e) {
		}

		return fieldValue != null;
	}

	/**
	 * Retrieve value from jsonMap by field sequence.
	 * @param jsonMap       source json object deserialized in map
	 * @param fieldSequence Sequence to field value.
	 *                      Example: person.car.number have to be ["person", "car[2]", "number"]
	 * @return Field value
	 */
	public static Object getField(Map<String, Object> jsonMap, String[] fieldSequence) {
		if (fieldSequence == null) {
			return null;
		}
		Map<String, Object> _jsonMap = jsonMap;
		for (int i = 0; i < fieldSequence.length; i++) {
			String fieldName = fieldSequence[i];
			//If elementIndexes not null it means that current field represent json array NOT object.
			List<Integer> elementIndexes = retrieveArrayIndexes(fieldName);
			Object fieldValue = null;
			if (elementIndexes != null) {
				fieldValue = retrieveFieldFromArrayInMap(_jsonMap, elementIndexes, fieldName);
			} else {
				fieldValue = _jsonMap.get(fieldName);
			}
			if (i < fieldSequence.length - 1) {
				if (fieldValue instanceof Map) {
					_jsonMap = (Map) fieldValue;
				} else {
					throw new IllegalArgumentException(
							"Not found field by sequence: " + Arrays.asList(fieldSequence) + " In: " + jsonMap);
				}
			} else {
				return fieldValue;
			}
		}

		return null;
	}

	/**
	 * Set value to json field (path to field presented as sequence of string)
	 *
	 * @param jsonMap       source json object deserialized in map
	 * @param fieldSequence Sequence to field value.
	 *                      *                      Example: person.car.number have to be ["person", "car[2]", "number"]
	 * @param field         field value
	 */
	public static void setField(Map<String, Object> jsonMap, List<String> fieldSequence, Object field) {
		setField(jsonMap, fieldSequence.toArray(new String[fieldSequence.size()]), field);
	}

	/**
	 * Set value to json field (path to field presented as sequence of string)
	 *
	 * @param jsonMap       source json object deserialized in map
	 * @param fieldSequence Sequence to field value.
	 *                      *                      Example: person.car.number have to be ["person", "car[2]", "number"]
	 * @param field         field value
	 */
	public static void setField(Map<String, Object> jsonMap, String[] fieldSequence, Object field) {
		if (fieldSequence == null || fieldSequence.length == 0) {
			throw new IllegalArgumentException("Field sequence is empty. Set value to root not possible.");
		}

		List<String> _fieldSequence = Arrays.asList(fieldSequence);

		Map<String, Object> _jsonMap = jsonMap;
		Iterator<String> iterator = _fieldSequence.iterator();
		while (iterator.hasNext()) {
			String fieldName = iterator.next();
			//If elementIndexes not null it means that current field represent json array NOT object.
			List<Integer> elementIndexes = retrieveArrayIndexes(fieldName);
			Object fieldValue = null;
			if (elementIndexes != null) {
				//If field don't exist throw exception
				fieldValue = retrieveFieldFromArrayInMap(_jsonMap, elementIndexes, fieldName);
			} else {
				fieldValue = _jsonMap.get(fieldName);
			}
			//If current field is NULL and represent json object and has inner elements
			//We have to create new Map that represent json object
			if (fieldValue == null && iterator.hasNext()) {
				Map<String, Object> newJsonMap = new TreeMap<>();
				_jsonMap.put(fieldName, newJsonMap);
				_jsonMap = newJsonMap;
			} else if (iterator.hasNext()) {
				if (fieldValue instanceof Map) {
					_jsonMap = (Map) fieldValue;
				} else {
					throw new IllegalArgumentException(
							"Not found field by sequence: " + Arrays.asList(fieldSequence) + " In: " + jsonMap);
				}
			} else {
				if (elementIndexes == null) {
					if (field == null) {
						_jsonMap.remove(fieldName);
					} else {
						_jsonMap.put(fieldName, field);
					}
				} else {
					int lastIndex = elementIndexes.remove(elementIndexes.size() - 1);
					fieldValue = retrieveFieldFromArrayInMap(_jsonMap, elementIndexes, fieldName);
					if (field == null) {
						((List)fieldValue).remove(lastIndex);
					} else {
						((List)fieldValue).set(lastIndex, field);
					}
				}
			}
		}

	}

	/**
	 * Retrieve value from jsonMap by field sequence.
	 *
	 * @param jsonMap       source json object deserialized in map
	 * @param fieldSequence Sequence to field value.
	 *                      Example: person.car.number have to be ["person", "car[2]", "number"]
	 * @return Field value
	 */
	public static Object getField(Map<String, Object> jsonMap, List<String> fieldSequence) {
		return getField(jsonMap, fieldSequence.toArray(new String[fieldSequence.size()]));
	}

	/**
	 * Delete field value from json Map (field path presented as sequence of string)
	 *
	 * @param jsonMap       source json object deserialized in map
	 * @param fieldSequence Sequence to field value.
	 *                      Example: person.car.number have to be ["person", "car[2]", "number"]
	 */
	public static void deleteField(Map<String, Object> jsonMap, List<String> fieldSequence) {
		if (fieldSequence == null || fieldSequence.isEmpty()) {
			throw new IllegalArgumentException("Field sequence is empty. Delete whole map is not allowed.");
		}
		if (fieldSequence.size() == 1) {
			String deletedFieldName = fieldSequence.get(0);
			jsonMap.remove(deletedFieldName);
			return;
		}

		List<String> _fieldSequence = new ArrayList<>(fieldSequence);
		String deletedFieldName = _fieldSequence.remove(_fieldSequence.size() - 1);
		Object targetJsonElement = getField(jsonMap, _fieldSequence);
		if (!(targetJsonElement instanceof Map)) {
			throw new IllegalArgumentException("Delete allowed only from json object.");
		}

		((Map) targetJsonElement).remove(deletedFieldName);
	}

	/**
	 * Retrieve array from Json Map then retrieve value from this array by indexes.
	 * @param jsonMap source json object deserialized in map
	 * @param elementIndexes [3, 4, 5, 6]
	 * @param fieldName example: array[0][2][3]
	 * @return field value
	 */
	private static Object retrieveFieldFromArrayInMap(Map<String, Object> jsonMap, List<Integer> elementIndexes,
													  String fieldName) {
		String _fieldName = fieldName;
		if (fieldName.contains("[")) {
			_fieldName = fieldName.substring(0, fieldName.indexOf("["));
		}
		Object fieldValue = jsonMap.get(_fieldName);
		for (Integer elementIndex : elementIndexes) {
			if (fieldValue instanceof List) {
				fieldValue = ((List) fieldValue).get(elementIndex);
			} else {
				throw new IllegalArgumentException("Expected that: " + fieldName + " is array.");
			}
		}

		return fieldValue;
	}

	/**
	 * Retrieve array of indexes from field name.
	 * Example: fieldName = 'arr[0][5][8]', method will return [0,5,8]
	 * @param fieldName field name with square brackets
	 * @return Array of indexes
	 */
	private static List<Integer> retrieveArrayIndexes(String fieldName) {
		List<Integer> elementIndexes = null;
		if (fieldName.matches(ARRAY_FIELD_REGEX)) {
			int startIndex = fieldName.indexOf("[");
			String indexesPart = fieldName.substring(startIndex);

			elementIndexes = new ArrayList<>();
			Matcher matcher = ARRAY_INDEXES_PATTERN.matcher(indexesPart);
			while (matcher.find()) {
				String index = indexesPart.substring(matcher.start() + 1, matcher.end() - 1);
				elementIndexes.add(Integer.valueOf(index));
			}
		}

		return elementIndexes;
	}

}
