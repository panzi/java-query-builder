package io.github.panzi.sql;

import java.sql.SQLException;

public class RecordNotFoundException extends SQLException {
	private static final long serialVersionUID = 1L;

	public RecordNotFoundException() {
	}

	public RecordNotFoundException(String reason) {
		super(reason);
	}

	public RecordNotFoundException(Throwable cause) {
		super(cause);
	}

	public RecordNotFoundException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public RecordNotFoundException(String reason, Throwable cause) {
		super(reason, cause);
	}

	public RecordNotFoundException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public RecordNotFoundException(String reason, String sqlState, Throwable cause) {
		super(reason, sqlState, cause);
	}

	public RecordNotFoundException(String reason, String sqlState, int vendorCode, Throwable cause) {
		super(reason, sqlState, vendorCode, cause);
	}

}
