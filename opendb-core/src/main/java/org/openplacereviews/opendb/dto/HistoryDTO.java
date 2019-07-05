package org.openplacereviews.opendb.dto;

import org.openplacereviews.opendb.ops.OpObject;

import java.util.List;
import java.util.Map;

public class HistoryDTO {

	public static class HistoryObjectRequest {
		public String historyType;
		public List<String> key;
		public int limit;
		public String sort;
		public Map<List<String>, List<HistoryDTO.HistoryEdit>> historySearchResult;

		public HistoryObjectRequest(String historyType, List<String> key, int limit, String sort) {
			this.historyType = historyType;
			this.key = key;
			this.limit = limit;
			this.sort = sort;
		}
	}

	public static class HistoryEdit {
		private List<String> id;
		private List<String> userId;
		private String objType;
		private OpObject objEdit;
		private OpObject deltaChanges;
		private String date;
		private Status status;

		public HistoryEdit(List<String> userId, String objType, OpObject objEdit, OpObject deltaChanges, String date, Status status) {
			this.userId = userId;
			this.objType = objType;
			this.objEdit = objEdit;
			this.deltaChanges = deltaChanges;
			this.date = date;
			this.status = status;
		}

		public List<String> getId() {
			return id;
		}

		public void setId(List<String> id) {
			this.id = id;
		}

		public List<String> getUserId() {
			return userId;
		}

		public void setUserId(List<String> userId) {
			this.userId = userId;
		}

		public String getObjType() {
			return objType;
		}

		public void setObjType(String objType) {
			this.objType = objType;
		}

		public OpObject getObjEdit() {
			return objEdit;
		}

		public void setObjEdit(OpObject objEdit) {
			this.objEdit = objEdit;
		}

		public OpObject getDeltaChanges() {
			return deltaChanges;
		}

		public void setDeltaChanges(OpObject deltaChanges) {
			this.deltaChanges = deltaChanges;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public Status getStatus() {
			return status;
		}

		public void setStatus(Status status) {
			this.status = status;
		}
	}

	public enum Status {
		CREATED(0),
		DELETED(1),
		EDITED(2),
		NOT_SPECIFIED(3);

		private final int value;

		Status(final int newValue) {
			value = newValue;
		}

		public int getValue() {
			return value;
		}

		public static Status getStatus(int value) {
			switch (value) {
				case 0: {
					return CREATED;
				}
				case 1: {
					return DELETED;
				}
				case 2: {
					return EDITED;
				}
				default: {
					return NOT_SPECIFIED;
				}
			}
		}
	}
}
