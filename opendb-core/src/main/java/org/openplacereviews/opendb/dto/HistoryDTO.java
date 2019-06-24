package org.openplacereviews.opendb.dto;

import org.openplacereviews.opendb.ops.OpObject;

import java.util.List;

public class HistoryDTO {

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

	public HistoryDTO setId(List<String> id) {
		this.id = id;
		return this;
	}

	public List<String> getUser() {
		return user;
	}

	public HistoryDTO setUser(List<String> user) {
		this.user = user;
		return this;
	}

	public String getDate() {
		return date;
	}

	public HistoryDTO setDate(String date) {
		this.date = date;
		return this;
	}

	public Status getStatus() {
		return status;
	}

	public HistoryDTO setStatus(Status status) {
		this.status = status;
		return this;
	}

	public OpObject getOpObject() {
		return opObject;
	}

	public HistoryDTO setOpObject(OpObject opObject) {
		this.opObject = opObject;
		return this;
	}

	public String getType() {
		return type;
	}

	public HistoryDTO setType(String type) {
		this.type = type;
		return this;
	}
}
