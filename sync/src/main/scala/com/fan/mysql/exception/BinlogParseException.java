package com.fan.mysql.exception;

import org.apache.commons.lang3.exception.ContextedRuntimeException;

public class BinlogParseException extends ContextedRuntimeException {

    private static final long serialVersionUID = -140876776030668838L;

    public BinlogParseException(String errorCode) {
        super(errorCode);
    }

    public BinlogParseException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

}
