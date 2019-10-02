package org.openplacereviews.opendb.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

@Component
public class ResponseEntityUtils {

	@Autowired
	private JsonFormatter formatter;

	public enum ResponseStatus {
		OK, FAILED, ERROR
	}

	public static class ResponseUserBody {
		public ResponseStatus status;
		public String msg;
		public String error;

		public ResponseUserBody(ResponseStatus status, String msg) {
			this.status = status;
			this.msg = msg;
		}

	}

	public String status(ResponseStatus status, String msg) {
		ResponseUserBody responseUserBody = new ResponseUserBody(status, msg);
		return formatter.fullObjectToJson(responseUserBody);
	}

	public ResponseEntity<String> ok(String msg) {
		return ResponseEntity.ok(status(ResponseStatus.OK, msg));
	}

	public ResponseEntity<String> ok() {
		return ok(null);
	}

	public ResponseEntity<String> error(String msg) {
		return ResponseEntity.ok(status(ResponseStatus.ERROR, msg));
	}

	public ResponseEntity<String> error() {
		return error(null);
	}

	public ResponseEntity<String> unauthorized(String msg) {
		return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
				.body(status(ResponseStatus.ERROR, msg));
	}

	public ResponseEntity<String> badRequest(String msg) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(status(ResponseStatus.ERROR, msg));
	}

	public ResponseEntity<String> failed(String msg) {
		return ResponseEntity.ok(status(ResponseStatus.FAILED, msg));
	}


}
