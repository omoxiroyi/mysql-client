package com.dingcloud.dts.binlog.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class BinlogParseException extends NestableRuntimeException {

	private static final long serialVersionUID = -140876776030668838L;

	public BinlogParseException(String errorCode) {
		super(errorCode);
	}

	public BinlogParseException(String errorCode, Throwable cause) {
		super(errorCode, cause);
	}

}
