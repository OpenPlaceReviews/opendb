package org.openplacereviews.opendb;

public class OUtils {

	
	public static boolean isEmpty(String s) {
		return s == null || s.trim().length() == 0;
	}
	
	public static boolean validateSqlIdentifier(String id, StringBuilder errorMessage, String field, String action) {
		if(isEmpty(id)) {
			errorMessage.append(String.format("Field '%s' is not specified which is necessary to %s", field, action));
			return false;
		}
		if(isValidJavaIdentifier(id)) {
			errorMessage.append(String.format("Value '%s' is not valid for %s to %s", id, field, action));
			return false;
		}
		return true;
	}
	
	public static boolean isValidJavaIdentifier(String s) {
	    if (s.isEmpty()) {
	        return false;
	    }
	    if (!Character.isJavaIdentifierStart(s.charAt(0))) {
	        return false;
	    }
	    for (int i = 1; i < s.length(); i++) {
	        if (!Character.isJavaIdentifierPart(s.charAt(i))) {
	            return false;
	        }
	    }
	    return true;
	}

	public static boolean equals(String s1, String s2) {
		if(s1 == null) {
			return s1 == s2;
		}
		return s1.equals(s2);
	}

}
