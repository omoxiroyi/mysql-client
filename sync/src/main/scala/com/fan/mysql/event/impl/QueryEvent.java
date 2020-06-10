package com.dingcloud.dts.binlog.event.impl;

import com.dingcloud.dts.binlog.event.AbstractEvent;
import com.dingcloud.dts.binlog.event.EventHeader;
import com.dingcloud.dts.binlog.parser.QueryEventParser.StatusVariable;

public class QueryEvent extends AbstractEvent {

	private long threadId;
	private long execTime;
	private int databaseLength;
	private int errorCode;
	private int statusVariablesLength;
	private StatusVariable statusVariables;
	private String databaseName;
	private String query;

	public QueryEvent(EventHeader header) {
		super(header);
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public long getExecTime() {
		return execTime;
	}

	public void setExecTime(long execTime) {
		this.execTime = execTime;
	}

	public int getDatabaseLength() {
		return databaseLength;
	}

	public void setDatabaseLength(int databaseLength) {
		this.databaseLength = databaseLength;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public int getStatusVariablesLength() {
		return statusVariablesLength;
	}

	public void setStatusVariablesLength(int statusVariablesLength) {
		this.statusVariablesLength = statusVariablesLength;
	}

	public StatusVariable getStatusVariables() {
		return statusVariables;
	}

	public void setStatusVariables(StatusVariable statusVariables) {
		this.statusVariables = statusVariables;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}
