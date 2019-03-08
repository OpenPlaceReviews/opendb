package org.openplacereviews.opendb.ops;

import java.util.Map;
import java.util.TreeMap;

public class ValidationTimer {
	public static final String OP_PREPARATION = "op_prepare";
	public static final String OP_VALIDATION = "op_all_valid";
	public static final String OP_SIG = "op_sigs";
	public static final String OP_ROLES = "op_roles";
	
	public static final String BLOCK_HEADER_VALID = "bl_header_valid";
	
	public static final String BLC_REBASE = "blc_rebase";
	public static final String BLC_ADD_OPERATIONS = "blc_add_operations";
	public static final String BLC_NEW_BLOCK = "blc_new_block";
	public static final String BLC_COMPACT = "blc_compact";
	public static final String BLC_TOTAL_BLOCK = "blc_total_block";
	
	private Map<String, Long> times = new TreeMap<>();
	
	long[] timings = new long[10];
	int length = 0;

	public ValidationTimer() {
	}
	
	public ValidationTimer start() {
		timings[0] = System.currentTimeMillis();
		return this;
	}
	
	public int startExtra() {
		int ind = ++length;
		if(length > timings.length) {
			throw new UnsupportedOperationException();
		}
		timings[ind] = System.currentTimeMillis();
		return ind;
	}

	public void measure(String name) {
		long l = System.currentTimeMillis() - timings[0];
		times.put(name, l);
	}
	
	public void measure(int ind, String name) {
		if (length == ind + 1) {
			length--;
		}
		long l = System.currentTimeMillis() - timings[ind];
		times.put(name, l);
	}
	
	public Map<String, Long> getTimes() {
		return times;
	}
}
