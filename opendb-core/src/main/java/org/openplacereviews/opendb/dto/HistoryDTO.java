package org.openplacereviews.opendb.dto;

import org.openplacereviews.opendb.ops.OpObject;

import java.util.List;
import java.util.Map;

public class HistoryDTO {

	private Map<List<String>, List<HistoryObject>> id;

	public Map<List<String>, List<HistoryObject>> getId() {
		return id;
	}

	public void setId(Map<List<String>, List<HistoryObject>> id) {
		this.id = id;
	}

	public static class HistoryObject {
		public enum Status {
			CREATED(0),
			DELETED(1),
			EDITED(2),
			NOT_SPECIFIED(3);

			private final int value;

			Status(final int newValue) {
				value = newValue;
			}

			public int getValue() { return value; }

			public static Status getStatus(int value) {
				switch (value) {
					case 0 : {
						return CREATED;
					}
					case 1 : {
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

		private List<String> id;
		private String type;
		private List<String> user;
		private String date;
		private Status status;
		private OpObject opObject;

		public List<String> getId() {
			return id;
		}

		public HistoryObject setId(List<String> id) {
			this.id = id;
			return this;
		}

		public List<String> getUser() {
			return user;
		}

		public HistoryObject setUser(List<String> user) {
			this.user = user;
			return this;
		}

		public String getDate() {
			return date;
		}

		public HistoryObject setDate(String date) {
			this.date = date;
			return this;
		}

		public Status getStatus() {
			return status;
		}

		public HistoryObject setStatus(Status status) {
			this.status = status;
			return this;
		}

		public OpObject getOpObject() {
			return opObject;
		}

		public HistoryObject setOpObject(OpObject opObject) {
			this.opObject = opObject;
			return this;
		}

		public String getType() {
			return type;
		}

		public HistoryObject setType(String type) {
			this.type = type;
			return this;
		}
	}
}
